/*
 * Copyright 2011 10gen Inc.
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

package com.mongodb.hadoop.pig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class Util {
    public static File createLocalInputFile(String filename, String[] inputData) 
            throws IOException {
        File f = new File(filename);
        f.deleteOnExit();
        writeToFile(f, inputData);  
        return f;
    }
    
    public static void writeToFile(File f, String[] inputData) 
            throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new 
                FileOutputStream(f), "UTF-8"));
        for (int i=0; i<inputData.length; i++){
            pw.println(inputData[i]);
        }
        pw.close();
    }
}
