package com.mongodb.hadoop;

import com.mongodb.hadoop.testutils.BaseHadoopTest;

import java.io.File;
import java.io.FileFilter;

public class HadoopVersionFilter implements FileFilter {
    private final boolean findTestJar;
    private static final String PROD_FORMAT = String.format("-%s.jar", BaseHadoopTest.PROJECT_VERSION);
    private static final String TEST_FORMAT = String.format("%s-tests.jar", BaseHadoopTest.PROJECT_VERSION);

    public HadoopVersionFilter() {
        this(false);
    }

    public HadoopVersionFilter(final boolean findTestJar) {
        this.findTestJar = findTestJar;
    }

    @Override
    public boolean accept(final File pathname) {
        return findTestJar ? pathname.getName().endsWith(TEST_FORMAT) : pathname.getName().endsWith(PROD_FORMAT);
    }
}
