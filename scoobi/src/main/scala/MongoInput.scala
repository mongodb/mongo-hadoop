package com.mongodb.hadoop
package scoobi
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

import com.nicta.scoobi.{DList, WireFormat}
import org.apache.hadoop.io.ObjectWritable
import com.mongodb.hadoop.io.{DBObjectWritable, BSONWritable}
import com.nicta.scoobi.io.{InputConverter, DataSource}
import com.nicta.scoobi.impl.Configured
import org.apache.hadoop.mapreduce.Job
import com.mongodb.casbah.{MongoCollection, MongoCursor}
import org.bson.BSONObject
import com.mongodb.casbah.Imports._
import java.io.{DataInput, DataOutput}
import org.apache.commons.logging.LogFactory
import com.mongodb.hadoop.util.MongoConfigUtil


object MongoInput {

  def fromCollection[A : Manifest : WireFormat](collection: MongoCollection): DList[A] = {
    fromCursor(collection.find())
  }

  def fromCursor[A : Manifest : WireFormat](cursor: MongoCursor): DList[A] = {
    implicit val wf = new MongoWireFormat
    val converter = new InputConverter[Object, BSONObject, A] {
      def fromKeyValue(context: InputContext, key: Object, value: BSONObject) =
        value.asInstanceOf[A]
    }
    val source: DataSource[Object, BSONObject, A] = new MongoSource(cursor, converter)
    DList.fromSource(source)
  }

  class MongoWireFormat extends WireFormat[DBObject] {
    def fromWire(in: DataInput): DBObject = {
      log.info("[from] Input: " + in)
      val w = new DBObjectWritable()
      w.readFields(in)
      w
    }

    def toWire(x: DBObject, out: DataOutput) {
      log.info("[to] X: " + x + " out: " + out)
    }
  }

  class BSONWireFormat extends WireFormat[BSONObject] {
    def fromWire(in: DataInput): BSONObject = {
      log.info("[from] Input: " + in)
      val w = new BSONWritable()
      w.readFields(in)
      w
    }

    def toWire(x: BSONObject, out: DataOutput) {
      log.info("[to] X: " + x + " out: " + out)
    }
  }

  class MongoSource[A: Manifest : WireFormat](val cursor: MongoCursor,
                                              converter: InputConverter[Object, BSONObject, A])
        extends DataSource[Object, BSONObject, A] with Configured {

    val inputFormat = classOf[MongoInputFormat]

    def inputCheck() {}

    def inputConfigure(job: Job) {
      // Extract information to setup our Hadoop job
      // TODO - We currently cannot support authentication!
      val query = cursor.query
      val coll = cursor.underlying.getCollection
      val db = coll.getDB
      val conn = db.getMongo
      val addr = conn.getAddress

      val inputURI = "mongodb://%s:%s/%s.%s".format(addr.getHost, addr.getPort,
                                                    db.getName, coll.getName)

      log.info("*** Input URI: %s".format(inputURI))
      log.info("*** Input Query: %s".format(query))
      MongoConfigUtil.setInputURI( job.getConfiguration, inputURI )
      MongoConfigUtil.setQuery( job.getConfiguration, query )
      configure(job)
    }

    lazy val inputConverter = converter

    def inputSize(): Long = 0L // todo - we can't really get # of bytes from Mongo cursors

    protected def checkPaths {}
  }

  val log = LogFactory.getLog(MongoInput.getClass)
}
