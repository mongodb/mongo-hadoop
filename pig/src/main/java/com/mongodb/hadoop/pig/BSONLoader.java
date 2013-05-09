package com.mongodb.hadoop.pig;

import com.mongodb.*;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.BSONFileInputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.bson.*;
import org.bson.types.*;

public class BSONLoader extends LoadFunc {

    private static TupleFactory tf = TupleFactory.getInstance();
    private static final Log log = LogFactory.getLog(BSONLoader.class);
    private String location;
    protected RecordReader in = null;

    public BSONLoader(){}

    @Override
    public void setLocation(String location, Job job) throws IOException{
        this.location = location;
        BSONFileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat(){
        return new BSONFileInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split){
        this.in = reader;
    }

    @Override
    public Tuple getNext() throws IOException{
        try{
            if(!in.nextKeyValue()) return null;
            Object val = in.getCurrentValue();
            Tuple t = tf.newTuple(1);
            t.set(0, convertBSONtoPigType(val));
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            throw new ExecException( "Error while reading input", 6018);
        }

    }

    public static Object convertBSONtoPigType(Object o) throws ExecException{
        if(o == null){
            return null;
        } else if(o instanceof Number || o instanceof String){
            return o;
        } else if(o instanceof Date){
            return ((Date)o).getTime();
        } else if(o instanceof ObjectId){
            return o.toString();
        } else if(o instanceof BasicBSONList){
            BasicBSONList bl = (BasicBSONList)o;
            Tuple t = tf.newTuple(bl.size());
            for(int i = 0; i < bl.size(); i++){
                t.set(i, convertBSONtoPigType(bl.get(i)));
            }
            return t;
        } else if(o instanceof Map){
            //TODO make this more efficient for lazy objects?
            Map<String, Object> fieldsMap = (Map<String, Object>)o;
            HashMap<String,Object> pigMap = new HashMap(fieldsMap.size());
            for(Map.Entry<String, Object> field : fieldsMap.entrySet()){
                pigMap.put(field.getKey(), convertBSONtoPigType(field.getValue()));
            }
            return pigMap;
        } else {
            return o;
        }
        
    }


}
