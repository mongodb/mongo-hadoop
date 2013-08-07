package com.mongodb.hadoop.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import com.mongodb.hadoop.hive.output.HiveBSONFileRecordWriter;

import com.mongodb.hadoop.mapred.BSONFileOutputFormat;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * @author Sweetie
 * An OutputFormat that writes BSON files
 * 
 */
@SuppressWarnings("deprecation")
public class HiveBSONFileOutputFormat<K, V> 
    extends BSONFileOutputFormat<K, V> implements HiveOutputFormat<K, V>{

    private static final Log LOG = LogFactory.getLog(HiveBSONFileOutputFormat.class);
    

    /**
     * 
     * create the final output file
     * 
     * @param: jc
     *         finalOutputPath - the file that the output should be directed at 
     *         valueClass - the value class used to create
     *         tableProperties - the tableInfo for this file's corresponding table
     * @return: RecordWriter for the output file
     * 
     */
    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, 
            Path fileOutputPath,
            Class<? extends Writable> valueClass, 
            boolean isCompressed, 
            Properties tableProperties,
            Progressable progress) throws IOException {
        
        FileSystem fs = fileOutputPath.getFileSystem(jc);
        FSDataOutputStream outFile = fs.create(fileOutputPath);
        
        FSDataOutputStream splitFile = null;
        if (MongoConfigUtil.getBSONOutputBuildSplits(jc)) {
            Path splitPath = new Path(fileOutputPath.getParent(), "." + fileOutputPath.getName() + ".splits");
            splitFile = fs.create(splitPath);
        }
        
        long splitSize = BSONSplitter.getSplitSize(jc,  null);
        
        return new HiveBSONFileRecordWriter(outFile, splitFile, splitSize);
    }

}
