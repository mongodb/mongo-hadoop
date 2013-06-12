package com.mongodb.hadoop.examples.sensors;

import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.bson.*;

public class Sensors extends MongoTool{

    private static final Log log = LogFactory.getLog( MongoTool.class );

    String _jobName = "Sensors Aggregation";

    public static void main( final String[] pArgs ) throws Exception{
        Configuration conf = new Configuration();
        System.exit( ToolRunner.run( conf, new Sensors(), pArgs ));
    }

}


