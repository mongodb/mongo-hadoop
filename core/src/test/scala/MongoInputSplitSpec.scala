/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.hadoop
package test

import org.specs2._
import com.mongodb.{BasicDBObject, DBObject, MongoURI}
import com.mongodb.hadoop.input._
import java.io._


class MongoInputSplitSpec extends Specification {
  def is = "Mongo Input Splits should serialize/deserialize correctly" !
           serializationTest ^ end
  
  def serializationTest = {
    val split = new MongoInputSplit(new MongoURI("mongodb://localhost/test.in"),
                                    "_id",
                                    new BasicDBObject("x", 5),
                                    new BasicDBObject("username", 1),
                                    new BasicDBObject("creation_date", 1),
                                    new BasicDBObject("_id", 10), //min
                                    new BasicDBObject("_id", 20), //max
                                    10000,
                                    0,
                                    false)

    val bytesOut = new ByteArrayOutputStream
    val out = new DataOutputStream(bytesOut)
    split.write(out)


    val bytes = bytesOut.toByteArray


    val deserSplit = new MongoInputSplit()
    
    deserSplit.readFields(new DataInputStream(new ByteArrayInputStream(bytes)))

    deserSplit must_==(split)
  }
}
