package com.mongodb.hadoop.examples.sensors;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

@SuppressWarnings("deprecation")
public class SensorDataGenerator {
    private static final int NUM_DEVICES = 100;
    private static final int NUM_LOGS = NUM_DEVICES * 100;
    private static final List<String> TYPES = Arrays.asList("temp", "humidity", "pressure", "sound", "light");
    private static final Log LOG = LogFactory.getLog(SensorDataGenerator.class);


    double getRandomInRange(final int from, final int to, final int fixed) {
        return (Math.random() * (to - from) + from)/*.toFixed(fixed) * 1*/;
    }

    String getRandomString(final int len) {
        String possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        String randomString = "";
        for (int i = 0; i < len; i++) {
            int randomPoz = getRandomInt(1, possible.length());
            randomString += possible.substring(randomPoz, randomPoz + 1);
        }
        return randomString;
    }

    Date randomDate(final Date start, final Date end) {
        return new Date(start.getTime() + ((long) (Math.random() * (end.getTime() - start.getTime()))));
    }

    <T> T choose(final List<T> choices) {
        return choices.get(getRandomInt(0, choices.size()));
    }

    int getRandomInt(final int min, final int max) {
        return (int) Math.floor(Math.random() * (max - min) + min);
    }

    public void run() throws UnknownHostException {
        final List<Integer> models = new ArrayList<Integer>();
        final List<String> owners = new ArrayList<String>();
        final MongoClient client = new MongoClient();

        DB db = client.getDB("mongo_hadoop");
        DBCollection devices = db.getCollection("devices");
        DBCollection logs = db.getCollection("logs");

        if ("true" .equals(System.getenv("SENSOR_DROP"))) {
            LOG.info("Dropping sensor data");
            devices.drop();
            logs.drop();
            devices.createIndex(new BasicDBObject("devices", 1));
        }
        db.getCollection("logs_aggregate").createIndex(new BasicDBObject("devices", 1));

        if (logs.count() == 0) {
            for (int i = 0; i < 10; i++) {
                owners.add(getRandomString(10));
            }

            for (int i = 0; i < 10; i++) {
                models.add(getRandomInt(10, 20));
            }

            List<ObjectId> deviceIds = new ArrayList<ObjectId>();
            for (int i = 0; i < NUM_DEVICES; i++) {
                DBObject device = new BasicDBObject("_id", new ObjectId())
                                      .append("name", getRandomString(5) + getRandomInt(3, 5))
                                      .append("type", choose(TYPES))
                                      .append("owner", choose(owners))
                                      .append("model", choose(models))
                                      .append("created_at", randomDate(new Date(2000, 1, 1, 16, 49, 29), new Date()));
                deviceIds.add((ObjectId) device.get("_id"));
                devices.insert(device);
            }


            for (int i = 0; i < NUM_LOGS; i++) {
                if (i % 50000 == 0) {
                    LOG.info(format("Creating %d sensor log data entries: %d%n", NUM_LOGS, i));
                }
                BasicDBList location = new BasicDBList();
                location.add(getRandomInRange(-180, 180, 3));
                location.add(getRandomInRange(-90, 90, 3));
                DBObject log = new BasicDBObject("_id", new ObjectId())
                                   .append("d_id", choose(deviceIds))
                                   .append("v", getRandomInt(0, 10000))
                                   .append("timestamp", randomDate(new Date(2013, 1, 1, 16, 49, 29), new Date()))
                                   .append("loc", location);
                logs.insert(log);
            }
        }

    }
}
