// MongoOutputCommiter.java
/*
 * Copyright 2010 10gen Inc.
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

package com.mongodb.hadoop.mapred.output;

import org.apache.hadoop.mapred.*;

public class MongoOutputCommiter extends OutputCommitter {

    public void abortTask(TaskAttemptContext taskContext) {
        System.out.println("should abort task");
    }

    public void cleanupJob(JobContext jobContext) {
        System.out.println("should cleanup job");
    }

    public void commitTask(TaskAttemptContext taskContext) {
        System.out.println("should commit task");
    }

    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return true;
    }

    public void setupJob(JobContext jobContext) {
        System.out.println("should setup job");
    }

    public void setupTask(TaskAttemptContext taskContext) {
        System.out.println("should setup context");
    }
}
