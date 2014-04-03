package com.mongodb.hadoop.splitter;

import com.mongodb.MongoURI;
import com.mongodb.hadoop.input.MongoInputSplit;

public class TestMongoInputSplit extends MongoInputSplit {

    public TestMongoInputSplit(MongoURI inputURI) {
        this.inputURI = inputURI;
    }
}