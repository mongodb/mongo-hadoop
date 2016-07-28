package com.mongodb.hadoop.examples.shakespeare;

import com.mongodb.hadoop.GridFSInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.input.GridFSSplit;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * MapReduce job that counts the most common exclamations in his complete works.
 */
public class Shakespeare extends MongoTool {
    public static final int MAX_EXCLAMATION_WORDS = 3;
    public static final int MIN_OCCURRENCES = 5;

    public Shakespeare() {
        JobConf conf = new JobConf(new Configuration());
        if (MongoTool.isMapRedV1()) {
            // TODO
        } else {
            MongoConfigUtil.setInputFormat(conf, GridFSInputFormat.class);
            MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
        }
        MongoConfigUtil.setInputURI(
          conf,
          "mongodb://localhost:27017/mongo_hadoop.fs");
        // End-of-sentence punctuation with lookbehind, to keep delimiter.
        MongoConfigUtil.setGridFSDelimiterPattern(conf, "(?<=[.?!])");
        MongoConfigUtil.setMapper(conf, ShakespeareMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, Text.class);
        MongoConfigUtil.setReducer(conf, ShakespeareReducer.class);
        MongoConfigUtil.setOutputKey(conf, NullWritable.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        MongoConfigUtil.setOutputURI(
          conf,
          "mongodb://localhost:27017/mongo_hadoop.shakespeare.out");

        setConf(conf);
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new Shakespeare(), args));
    }

    static class ShakespeareMapper
      extends Mapper<NullWritable, Text, Text, Text> {
        private HashSet<String> secondPersonPronouns;
        private Text exclamation;
        private Text foundIn;

        public ShakespeareMapper() {
            super();
            secondPersonPronouns = new HashSet<String>() {{
                add("you");
                add("your");
                add("yours");
                add("ye");
                add("thou");
                add("thy");
                add("thine");
                add("thee");
            }};
            exclamation = new Text();
            foundIn = new Text();
        }

        private boolean isExclamation(final String test) {
            // Exclamations must end!
            if (!test.endsWith("!")) {
                return false;
            }
            String[] words = test.split("[\r\n\t ]+");
            // We figure the most entertaining exclamations will be directed at
            // the listener.
            for (String word : words) {
                if (secondPersonPronouns.contains(word)) {
                    return true;
                }
            }
            // Exclamations be brief!
            return words.length <= MAX_EXCLAMATION_WORDS;
        }

        @Override
        protected void map(final NullWritable key, final Text value, final Context context)
          throws IOException, InterruptedException {
            GridFSSplit gridSplit = (GridFSSplit) context.getInputSplit();

            // Work title will become the output key.
            String workTitle = (String) gridSplit.get("filename");

            // Extract exclamations.
            String sentence = Text.decode(value.getBytes()).trim().toLowerCase();
            if (isExclamation(sentence)) {
                foundIn.set(workTitle);
                exclamation.set(sentence);

                context.write(exclamation, foundIn);
            }
        }
    }

    static class ShakespeareReducer
      extends Reducer<Text, Text, NullWritable, BSONWritable> {
        private final BSONWritable bsonWritable;

        public ShakespeareReducer() {
            super();
            bsonWritable = new BSONWritable();
        }

        @Override
        protected void reduce(
          final Text key, final Iterable<Text> values,
          final Context context) throws IOException, InterruptedException {
            Map<String, Integer> foundInMap = new HashMap<String, Integer>();
            int totalCount = 0;
            for (Text foundIn : values) {
                String title = foundIn.toString();
                if (foundInMap.containsKey(title)) {
                    foundInMap.put(title, foundInMap.get(title) + 1);
                } else {
                    foundInMap.put(title, 1);
                }
                ++totalCount;
            }
            if (totalCount >= MIN_OCCURRENCES) {
                BSONObject result =
                  new BasicBSONObject("totalCount", totalCount);
                result.put("exclamation", key.toString());
                result.put("counts", foundInMap);
                result.put(
                  "wordCount", key.toString().split("[\r\n\t ]+").length);
                bsonWritable.setDoc(result);
                context.write(NullWritable.get(), bsonWritable);
            }
        }
    }
}
