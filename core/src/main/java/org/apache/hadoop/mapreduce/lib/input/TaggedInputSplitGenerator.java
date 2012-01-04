package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class TaggedInputSplitGenerator {
	public static TaggedInputSplit getTaggedInputSplit(InputSplit inputSplit, Configuration conf,
		      Class<? extends InputFormat> inputFormatClass,
		      Class<? extends Mapper> mapperClass){
		return new TaggedInputSplit(inputSplit, conf, inputFormatClass, mapperClass);
	}
}
