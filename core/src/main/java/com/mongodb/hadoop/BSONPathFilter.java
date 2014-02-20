/*
 * Copyright 2010-2013 10gen Inc.
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
 */
package com.mongodb.hadoop;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class BSONPathFilter implements PathFilter{

    private static final Log log = LogFactory.getLog( BSONPathFilter.class );

    public BSONPathFilter(){
        log.info("path filter constructed.");
    }

    public boolean accept(Path path){
        String pathName = path.getName().toLowerCase();
        boolean acceptable = pathName.endsWith(".bson") && !pathName.startsWith("."); 
        log.info(path.toString() + " returning " + acceptable);
        return acceptable;
    }
}

