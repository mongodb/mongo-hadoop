package org.mongodb.hadoop;

import com.mongodb.hadoop.testutils.BaseHadoopTest;

import java.io.File;
import java.io.FileFilter;

public class HadoopVersionFilter implements FileFilter {
    @Override
    public boolean accept(final File pathname) {
        return pathname.getName().endsWith(String.format("_%s.jar", BaseHadoopTest.HADOOP_VERSION));
    }
}
