/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.DelegatingRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.mongodb.hadoop.MongoInputFormat;

/**
 * An {@link InputFormat} that delegates behavior of paths to multiple other
 * InputFormats.
 * 
 * @see MultipleInputs#addInputPath(Job, Path, Class, Class)
 */
public class DelegatingInputFormat<K, V> extends InputFormat<K, V> {

  @SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
	  Configuration conf = job.getConfiguration();
	  Job jobCopy =new Job(conf);
	  List<InputSplit> splits = new ArrayList<InputSplit>();
	  Map<Path, InputFormat> formatMap = MongoMultipleInputs.getInputFormatMap(job);
	  Map<Path, Class<? extends Mapper>> mapperMap = MongoMultipleInputs.getMapperTypeMap(job);
//     Map<Class<? extends InputFormat>, List<Path>> formatPaths	= new HashMap<Class<? extends InputFormat>, List<Path>>();

	  for (Entry<Path, InputFormat> entry : formatMap.entrySet()) {
		  InputFormat formatClass = (InputFormat) ReflectionUtils.newInstance(entry.getValue().getClass(), conf);
		  Class<? extends Mapper> mapperClass;
		  mapperClass = mapperMap.get(entry.getKey());
		  try{
			  List<InputSplit> pathSplits = ((MongoInputFormat) formatClass).getSplits(jobCopy, entry.getKey());
			  for (InputSplit pathSplit : pathSplits) {
				  splits.add(new TaggedInputSplit(pathSplit, conf, formatClass.getClass(), mapperClass));
			  }
		  }catch(ClassCastException e){
			  List<InputSplit> pathSplits = formatClass.getSplits(jobCopy);
			  for (InputSplit pathSplit : pathSplits) {
				  splits.add(new TaggedInputSplit(pathSplit, conf, formatClass.getClass(), mapperClass));
			  }
		  }
	  }
	  return splits;
  }
			
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new DelegatingRecordReader<K, V>(split, context);
	}
	private static final Log log = LogFactory.getLog( DelegatingInputFormat.class );
}
