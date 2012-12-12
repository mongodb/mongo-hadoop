package com.mongodb.hadoop.input;

import com.mongodb.MongoURI;

public class TestMongoInputSplit extends MongoInputSplit {

    public TestMongoInputSplit(MongoURI inputURI) {
        _mongoURI = inputURI;
    }

}
