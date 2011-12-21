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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.DelegatingMapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoRequest;

/**
 * This class supports MapReduce jobs that have multiple input paths with
 * a different {@link InputFormat} and {@link Mapper} for each path 
 */
public class MongoMultipleInputs {
	
  /**
   * Add a {@link Path} with a custom {@link InputFormat} and
   * {@link Mapper} to the list of inputs for the map-reduce job.
   * 
   * @param job The {@link Job}
   * @param path {@link Path} to be added to the list of inputs for the job
   * @param inputFormatClass {@link InputFormat} class to use for this path
   * @param mapperClass {@link Mapper} class to use for this path
   * @param query {@link DBObject} query for path and mapper
   * @param fields {@link DBObject} fields for path and mapper
   * @param sort {@link DBObject} sort for path and mapper
   * @param limit {@link int} limit for path and mapper
   * @param skip {@link int} skip for path and mapper
   */
  @SuppressWarnings("unchecked")
  public static void addInputPath(Job job, String uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
	  Configuration conf = job.getConfiguration();
	  MongoConfigUtil.addMongoRequest(conf, uri, inputFormatClass, mapperClass, query, fields, sort, limit, skip);
	  
	  job.setMapperClass(DelegatingMapper.class);
	  job.setInputFormatClass(DelegatingInputFormat.class);
  }
  // job, uri, inputformat, mapper, query, fields, sort
  public static void addInputPath(Job job, MongoURI uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields, DBObject sort, int limit, int skip) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, fields, sort, limit, skip);
  }
  // job, uri, inputformat, mapper, query, fields, sort
  public static void addInputPath(Job job, String uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields, DBObject sort) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, fields, sort, 0, 0);
  }
  public static void addInputPath(Job job, MongoURI uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields, DBObject sort) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, fields, sort, 0, 0);
  }
  // job, uri, inputformat, mapper, query, fields
  public static void addInputPath(Job job, String uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, fields, null, 0, 0);
  }
  public static void addInputPath(Job job, MongoURI uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query, DBObject fields) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, fields, null, 0, 0);
  }
  // job, uri, inputformat, mapper, query
  public static void addInputPath(Job job, String uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, null, null, 0, 0);
  }
  public static void addInputPath(Job job, MongoURI uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass,
		  DBObject query) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, query, null, null, 0, 0);
  }
  // job, uri, inputformat, mapper
  public static void addInputPath(Job job, String uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, new BasicDBObject(), new BasicDBObject(), new BasicDBObject(), 0, 0);
  }
  public static void addInputPath(Job job, MongoURI uri, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass) {
	  addInputPath(job, uri.toString(), inputFormatClass, mapperClass, null, null, null, 0, 0);
  }
  /**
   * Retrieves a map of {@link Path}s to the {@link InputFormat} class
   * that should be used for them.
   * 
   * @param job The {@link JobContext}
   * @see #addInputPath(JobConf, Path, Class)
   * @return A map of paths to inputformats for the job
   */
  @SuppressWarnings("unchecked")
  static Map<Path, InputFormat> getInputFormatMap(JobContext job) {
    Map<Path, InputFormat> m = new HashMap<Path, InputFormat>();
    Configuration conf = job.getConfiguration();
    
    List<MongoRequest> mongoRequests = MongoConfigUtil.getMongoRequests(conf);
    for (MongoRequest mongoRequest : mongoRequests) {
      InputFormat inputFormat;
      try {
       inputFormat = (InputFormat) ReflectionUtils.newInstance(conf
           .getClassByName(mongoRequest.getInputFormat()), conf);
      } catch (ClassNotFoundException e) {
       throw new RuntimeException(e);
      }
      m.put(new Path(mongoRequest.getInputURI().toString()), inputFormat);
    }
    return m;
  }

  /**
   * Retrieves a map of {@link Path}s to the {@link Mapper} class that
   * should be used for them.
   * 
   * @param job The {@link JobContext}
   * @see #addInputPath(JobConf, Path, Class, Class)
   * @return A map of paths to mappers for the job
   */
  @SuppressWarnings("unchecked")
  static Map<Path, Class<? extends Mapper>> 
      getMapperTypeMap(JobContext job) {
    Configuration conf = job.getConfiguration();
    List<MongoRequest> mongoRequests = MongoConfigUtil.getMongoRequests(conf);
    if (mongoRequests == null) {
      return Collections.emptyMap();
    }
    Map<Path, Class<? extends Mapper>> m = new HashMap<Path, Class<? extends Mapper>>();
    for (MongoRequest mongoRequest : mongoRequests) {
      Class<? extends Mapper> mapClass;
      try {
       mapClass = (Class<? extends Mapper>) conf.getClassByName(mongoRequest.getMapper());
      } catch (ClassNotFoundException e) {
       throw new RuntimeException(e);
      }
      m.put(new Path(mongoRequest.getInputURI().toString()), mapClass);
    }
    return m;
  }
  private static final Log log = LogFactory.getLog( MongoMultipleInputs.class );

}
