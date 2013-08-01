package com.mongodb.hadoop.splitter;

public class SplitFailedException extends Exception{

    public SplitFailedException(String message){
        super(message);
    }

    public SplitFailedException(String message, Throwable cause){
        super(message, cause);
    }
}

