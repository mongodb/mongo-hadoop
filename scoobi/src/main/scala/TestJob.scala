package com.mongodb.hadoop
package scoobi
package test

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import com.mongodb.casbah.Imports._
import com.nicta.scoobi.Scoobi._
import org.bson.BSONObject
import com.mongodb.hadoop.scoobi.MongoInput.MongoWireFormat


object TestJob extends ScoobiApp {
  def run() {
    implicit val wf = new MongoWireFormat
    val data = MongoInput.fromCollection[BSONObject](MongoConnection()("playbookstore")("books"))
    val x = data.map(doc => (doc.get("title").asInstanceOf[String], 1)).groupByKey
    System.err.println(x)
    persist(toTextFile(x, "./test"))

  }

}
