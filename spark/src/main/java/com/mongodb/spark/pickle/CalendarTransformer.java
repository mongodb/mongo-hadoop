package com.mongodb.spark.pickle;

import org.bson.Transformer;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Transformer that turns java.util.Calendar objects into java.util.Date
 * objects.
 *
 * This class is needed because Spark constructs pickled Python
 * datetime.datetime objects into java.util.GregorianCalendar instances instead
 * of java.util.Date objects.
 */
public class CalendarTransformer implements Transformer {
    @Override
    public Object transform(final Object objectToTransform) {
        Calendar calendar = (Calendar) objectToTransform;
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        return calendar.getTime();
    }
}
