package com.mongodb.hadoop.hive;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class Results implements Iterable<List<String>> {

    public class Field {
        private final String name;
        private final String type;

        public Field(final String name, final String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() { return name; }

        public String getType() { return type; }

        //CHECKSTYLE:OFF
        public boolean equals(final Object other) {
            if (!(other instanceof Field)) {
                return false;
            }
            Field otherField = (Field) other;
            return otherField.getName().equals(name)
              && otherField.getType().equals(type);
        }
        //CHECKSTYLE:ON
    }

    private List<Field> fields;
    private List<List<String>> data = new ArrayList<List<String>>();
    private Exception error;

    public Results() {
    }

    public void process(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int numCols = metaData.getColumnCount();
        fields = new ArrayList<Field>(numCols);

        // Column numbering starts at 1.
        for (int i = 1; i <= numCols; i++) {
            String colName = metaData.getColumnName(i);
            // Column names are in the format <table name>.<column title>,
            // unless they've been aliased with AS.
            String[] nameParts = colName.split("\\.", 2);
            String fieldName = nameParts.length > 1 ? nameParts[1] : nameParts[0];
            fields.add(new Field(fieldName, metaData.getColumnTypeName(i)));
        }
        while (resultSet.next()) {
            List<String> rowData = new ArrayList<String>(numCols);
            for (int i = 1; i <= numCols; i++) {
                rowData.add(resultSet.getString(i));
            }
            data.add(rowData);
        }
    }

    public int size() {
        return data.size();
    }

    public List<String> get(final int i) {
        return data.get(i);
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (fields != null) {
            for (Field field : fields) {
                sb.append(format(" %15s   |", field.getName()));
            }
            sb.append("\n");
            for (List<String> row : data) {
                for (String s1 : row) {
                    sb.append(format(" %-15s   |", s1.trim()));
                }
                sb.append("\n");
            }
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

    public Map<String, String> getRow(final int row) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        List<String> strings = get(row);
        for (int i = 0; i < fields.size(); i++) {
            final Field field = fields.get(i);
            map.put(field.getName(), strings.get(i));
        }

        return map;
    }
}
