/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
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
package com.mongodb.hadoop
package test

import java.io.FileInputStream
import com.mongodb.hadoop.util.BSONLoader
import org.bson.BSONObject

class BSONFileSpec extends Specification {
  def is = "Reading BSON Files off the disk should you know, work... " !
    readBSONFile ^ end

  def readBSONFile() = {
    val fh = new FileInputStream("core/src/test/resources/bookstore-dump/inventory.bson")
    val ldr = new BSONLoader(fh)
    var n = 0
    for (doc <- ldr.asInstanceOf[java.util.Iterator[BSONObject]].asScala) {
      //println("Doc: " + doc)
      n += 1
    }
    n must beEqualTo(336)
  }
}