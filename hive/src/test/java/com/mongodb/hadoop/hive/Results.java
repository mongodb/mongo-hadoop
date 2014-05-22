package com.mongodb.hadoop.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class Results implements Iterable<List<String>> {

    private List<FieldSchema> fields;
    private List<List<String>> data = new ArrayList<List<String>>();
    private Exception error;

    public Results() {
    }

    public void process(final HiveClient client) throws TException {
        Schema schema = client.getSchema();
        fields = schema.getFieldSchemas();
        List<String> strings = client.fetchAll();
        for (String string : strings) {
            data.add(Arrays.asList(string.split("\t")));
        }
    }

    public void process(final Exception e) {
        error = e;
    }

    public boolean hasError() {
        return error != null;
    }

    public Exception getError() {
        return error;
    }

    public int size() {
        return data.size();
    }

    public List<String> get(final int i) {
        return data.get(i);
    }

    public List<FieldSchema> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (FieldSchema fieldSchema : fields) {
            sb.append(format(" %15s   |", fieldSchema.getName()));
        }
        sb.append("\n");
        for (List<String> row : data) {
            for (String s1 : row) {
                sb.append(format(" %-15s   |", s1.trim()));
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Results)) {
            return false;
        }

        final Results results = (Results) o;

        if (data != null ? !data.equals(results.data) : results.data != null) {
            return false;
        }
        if (fields != null ? !fields.equals(results.fields) : results.fields != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    @Override
    public Iterator<List<String>> iterator() {
        return data.iterator();
    }
}
