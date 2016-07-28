package com.mongodb.hadoop.streaming.io;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.OutputReader;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.DataInput;
import java.io.IOException;

/**
 * OutputReader that detects updates and emits a MongoUpdateWritable for them.
 */
public class MongoUpdateOutputReader
  extends OutputReader<BSONWritable, MongoUpdateWritable> {

    private final BSONWritable valueWritable = new BSONWritable();
    private final BSONWritable keyWritable = new BSONWritable();
    private final MongoUpdateWritable outputWritable =
      new MongoUpdateWritable();
    private DataInput input;

    @Override
    public void initialize(final PipeMapRed pipeMapRed) throws IOException {
        super.initialize(pipeMapRed);
        input = pipeMapRed.getClientInput();
    }

    @Override
    public boolean readKeyValue() throws IOException {
        valueWritable.readFields(input);
        Object id = valueWritable.getDoc().get("_id");
        if (null == id) {
            return false;
        }
        keyWritable.setDoc(new BasicBSONObject("_id", id));
        return true;
    }

    @Override
    public BSONWritable getCurrentKey() throws IOException {
        return keyWritable;
    }

    private boolean getBoolean(
      final BSONObject obj, final String key, final boolean fallback) {
        return (Boolean) (null == obj.get(key) ? fallback : obj.get(key));
    }

    private void initializeOutputWritable(final BasicBSONObject value)
      throws IOException {
        if (!(value.containsField("modifiers") && value.containsField("_id"))) {
            outputWritable.setQuery(value);
            return;
        }

        Object query = value.get("_id");
        if (query instanceof BasicBSONObject) {
            outputWritable.setQuery((BasicBSONObject) query);
        } else {
            throw new IOException(
              "_id must be a document describing the query of the update, not "
                + query);
        }
        Object modifiers = value.get("modifiers");
        if (modifiers instanceof BasicBSONObject) {
            outputWritable.setModifiers((BasicBSONObject) modifiers);
        } else {
            throw new IOException(
              "modifiers must be a replacement or update document, not"
                + modifiers);
        }
        Object options = value.get("options");
        if (options instanceof BSONObject) {
            BSONObject updateOptions = (BSONObject) value.get("options");
            outputWritable.setUpsert(getBoolean(updateOptions, "upsert", true));
            outputWritable.setMultiUpdate(
              getBoolean(updateOptions, "multi", false));
            outputWritable.setReplace(
              getBoolean(updateOptions, "replace", false));
        } else if (options != null) {
            throw new IOException(
              "options must either be null or a document providing update "
                + "options, not " + options);
        }
    }

    @Override
    public MongoUpdateWritable getCurrentValue() throws IOException {
        // Try to guess if the output describes an update. If it does, fill
        // out the MongoUpdateWritable accordingly. Otherwise, fill out only
        // the "query" portion of the MUW.
        initializeOutputWritable((BasicBSONObject) valueWritable.getDoc());
        return outputWritable;
    }

    @Override
    public String getLastOutput() {
        return valueWritable.toString();
    }
}
