package com.mongodb.hadoop.splitter;

public class SplitFailedException extends Exception {

    public SplitFailedException(final String message) {
        super(message);
    }

    public SplitFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

