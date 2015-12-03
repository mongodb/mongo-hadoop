package com.mongodb.spark.examples.enron;

import com.mongodb.hadoop.MongoInputFormat;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.bson.BSONObject;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 12/3/15.
 */
public class Enron {

    public static void main(final String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        // Set configuration options for the MongoDB Hadoop Connector.
        Configuration mongodbConfig = new Configuration();
        // MongoInputFormat allows us to read from a live MongoDB instance.
        // We could also use BSONFileInputFormat to read BSON snapshots.
        mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

        // MongoDB connection string naming a collection to use.
        // If using BSON, use "mapred.input.dir" to configure the directory
        // where BSON files are located instead.
        mongodbConfig.set("mongo.input.uri",
                "mongodb://localhost:27017/enron_mail.messages");

        // Create an RDD backed by the MongoDB collection.
        JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
                mongodbConfig,            // Configuration
                MongoInputFormat.class,   // InputFormat: read from a live cluster.
                Object.class,             // Key class
                BSONObject.class          // Value class
        );

        JavaRDD<String> edges = documents.flatMap(

                new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {

                    @Override
                    public Iterable<String> call(final Tuple2<Object, BSONObject> t) throws Exception {

                        BSONObject header = (BSONObject) t._2.get("headers");
                        String to = (String) header.get("To");
                        String from = (String) header.get("From");

                        // each tuple in the set is an individual from|to pair
                        //JavaPairRDD<String, Integer> tuples = new JavaPairRDD<String, Integer>();
                        List<String> tuples = new ArrayList<String>();

                        if (to != null && !to.isEmpty()) {
                            for (String recipient : to.split(",")) {
                                String s = recipient.trim();
                                if (s.length() > 0) {
                                    tuples.add(from + "|" + s);
                                }
                            }
                        }
                        return tuples;
                    }
                }
        );

        JavaPairRDD<String, Integer> pairs = edges.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(final String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(final Integer a, final Integer b) {
                        return a + b;
                    }
                }
        );

        // Create a separate Configuration for saving data back to MongoDB.
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri",
                "mongodb://localhost:27017/enron_mail.message_pairs");

        // Save this RDD as a Hadoop "file".
        // The path argument is unused; all documents will go to 'mongo.output.uri'.
        counts.saveAsNewAPIHadoopFile(
                "file:///this-is-completely-unused",
                Object.class,
                BSONObject.class,
                MongoOutputFormat.class,
                outputConfig
        );

    }
}
