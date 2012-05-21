# Mongo-Hadoop Ruby Streaming Support

This Gem provides Ruby language support for the MongoDB Hadoop Streaming Assembly.

## Installation

First, make sure you have built the [MongoDB Hadoop Adapter]("http://api.mongodb.org/hadoop/Building+the+Adapter.html") and [Streaming Support](http://api.mongodb.org/hadoop/Building+Hadoop+Streaming+Support.html).

Then `gem install mongo-hadoop` and start creating mappers and reducers. If you need some ideas, there are examples in the [github repository](https://github.com/mongodb/mongo-hadoop/tree/master/streaming/examples).

## Mappers

The current Ruby implementation of streaming requires that the user define and pass a block to serve as a map function to the `MongoHadoop.map` or `MongoHadoop.kvmap` methods.

### `map`

The block passed to `map` takes a single argument which is the document to be processed by the mapper function. It needs to then emit (return) one document.

### `kvmap`

The key-value mapper, `kvmap`, takes two arguments, a key and a value, and must return an array in which the first element is the key to be emitted and the second is a value docuement.

### Example

    #!/usr/bin/env ruby
    require 'mongo-hadoop'

    MongoHadoop.map do |document|
      { :_id => doc['_id'], :value => doc['somefield'] }
    end

## Reducers

Creating a reducer is similar. The user defines a block to serve as the reduce function.

### `reduce`

The block passed to `reduce` should take two arguments, a key and an array of values. It should iterate over the values and reduce the data by producing a single document that has an `_id` field with the key and other fields with some reduction of the data (sum, count, etc...)

### `kvreduce`

The key-value reducer should function similarly in that it takes a key and array of values but returns an array with the key as the first element and a value as the second.


### Example
    
    #!/usr/bin/env ruby
    require 'mongo-hadoop'

    MongoHadoop.reduce do |key, values|
      count = sum = 0
  
      values.each do |value|
        count += 1
        sum += value['num']
      end

      { :_id => key, :average => sum / count }
    end

## Running Map-Reduce with Hadoop Streaming

To run your data through these map-reduce functions first define the mapper and reducer in seperate files and make them executable.

Also define an executable shell script which runs the following:

`hadoop jar <location of streaming assembly jar> -mapper <your mapper> -reducer <your reducer> -inputURI <uri of input collection> -outputURI <uri of output collection>`

