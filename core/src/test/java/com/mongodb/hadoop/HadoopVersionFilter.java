package com.mongodb.hadoop;

import com.mongodb.hadoop.testutils.BaseHadoopTest;

import java.io.File;
import java.io.FileFilter;

public class HadoopVersionFilter implements FileFilter {
    private final boolean findTestJar;
    private static final String FORMAT = String.format("-%s.jar", BaseHadoopTest.PROJECT_VERSION);

    public HadoopVersionFilter() {
        this(false);
    }

    public HadoopVersionFilter(final boolean findTestJar) {
        this.findTestJar = findTestJar;
    }

    @Override
    public boolean accept(final File pathname) {
        return findTestJar ? pathname.getName().contains("-test-")
                           : pathname.getName().endsWith(FORMAT);
    }
}
