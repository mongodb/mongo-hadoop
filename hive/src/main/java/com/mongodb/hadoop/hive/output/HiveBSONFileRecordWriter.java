package com.mongodb.hadoop.hive.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskAttemptContext;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.output.BSONFileRecordWriter;

@SuppressWarnings("deprecation")
public class HiveBSONFileRecordWriter<K, V> extends BSONFileRecordWriter<K, V> 
implements RecordWriter {

    public HiveBSONFileRecordWriter(FSDataOutputStream outFile,
            FSDataOutputStream splitFile, long splitSize) {
        super(outFile, splitFile, splitSize);
    }

    @Override
    public void close(boolean toClose) throws IOException {
        this.close(toClose ? (TaskAttemptContext) new Object() : (TaskAttemptContext) null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Writable value) throws IOException {
        write(null, (BSONWritable) value);
    }

}
