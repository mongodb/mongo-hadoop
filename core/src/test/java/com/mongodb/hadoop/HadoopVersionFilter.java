package com.mongodb.hadoop;

import com.mongodb.hadoop.testutils.BaseHadoopTest;

import java.io.File;
import java.io.FileFilter;

public class HadoopVersionFilter implements FileFilter {
    private final boolean findTestJar;

    public HadoopVersionFilter() {
        this(false);
    }
    
    public HadoopVersionFilter(final boolean findTestJar) {
        this.findTestJar = findTestJar;
    }

    @Override
    public boolean accept(final File pathname) {
        String format = String.format("_%s.jar", BaseHadoopTest.HADOOP_VERSION);
        
        return findTestJar ? pathname.getName().contains("-test-")
                           : pathname.getName().endsWith(format);
    }
}
