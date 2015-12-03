package com.mongodb.spark.examples.enron;


import com.mongodb.hadoop.MongoInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import org.bson.BSONObject;
import scala.Tuple2;


/**
 * Created by bryan on 12/3/15.
 */
public class DataframeExample {
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

            JavaRDD<Message> messages = documents.map(

                    new Function<Tuple2<Object, BSONObject>, Message>() {

                        public Message call(final Tuple2<Object, BSONObject> tuple) {
                            Message m = new Message();
                            BSONObject header = (BSONObject) tuple._2.get("headers");

                            m.setTo((String) header.get("To"));
                            m.setxFrom((String) header.get("From"));
                            m.setMessageID((String) header.get("Message-ID"));
                            m.setBody((String) tuple._2.get("body"));

                            return m;
                        }
                    }
            );

            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

            DataFrame messagesSchema = sqlContext.createDataFrame(messages, Message.class);
            messagesSchema.registerTempTable("messages");

            DataFrame ericsMessages = sqlContext.sql("SELECT to, body FROM messages WHERE to = \"eric.bass@enron.com\"");

            ericsMessages.show();

            messagesSchema.printSchema();
        }

}
