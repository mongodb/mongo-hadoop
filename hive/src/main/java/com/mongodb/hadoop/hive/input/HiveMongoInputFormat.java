/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.hive.input;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hadoop.hive.BSONSerDe;
import com.mongodb.hadoop.hive.MongoStorageHandler;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.input.MongoRecordReader;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.bson.BSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*
 * Defines a HiveInputFormat for use in reading data from MongoDB into a hive table
 * 
 */
public class HiveMongoInputFormat extends HiveInputFormat<BSONWritable, BSONWritable> {

    private static final String EQUAL_OP = GenericUDFOPEqual.class.getName();
    private static final Map<String, String> MONGO_OPS =
      new HashMap<String, String>() {{
          put(GenericUDFOPLessThan.class.getName(), "$lt");
          put(GenericUDFOPEqualOrLessThan.class.getName(), "$lte");
          put(GenericUDFOPGreaterThan.class.getName(), "$gt");
          put(GenericUDFOPEqualOrGreaterThan.class.getName(), "$gte");
      }};

    private static final Log LOG = LogFactory.getLog(HiveMongoInputFormat.class);

    @Override
    public RecordReader<BSONWritable, BSONWritable> getRecordReader(final InputSplit split,
                                                                    final JobConf conf,
                                                                    final Reporter reporter)
        throws IOException {

        // split is of type 'MongoHiveInputSplit'
        MongoHiveInputSplit mhis = (MongoHiveInputSplit) split;

        // Get column name mapping.
        Map<String, String> colToMongoNames = columnMapping(conf);

        // Add projection from Hive.
        DBObject mongoProjection = getProjection(conf, colToMongoNames);
        MongoInputSplit delegate = (MongoInputSplit) mhis.getDelegate();
        if (mongoProjection != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding MongoDB projection : " + mongoProjection);
            }
            delegate.setFields(mongoProjection);
        }
        // Filter from Hive.
        DBObject filter = getFilter(conf, colToMongoNames);
        // Combine with filter from table, if there is one.
        if (conf.get(MongoConfigUtil.INPUT_QUERY) != null) {
            DBObject tableFilter = MongoConfigUtil.getQuery(conf);
            if (null == filter) {
                filter = tableFilter;
            } else {
                BasicDBList conditions = new BasicDBList();
                conditions.add(filter);
                conditions.add(tableFilter);
                // Use $and clause so we don't overwrite any of the table
                // filter.
                filter = new BasicDBObject("$and", conditions);
            }
        }
        if (filter != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding MongoDB query: " + filter);
            }
            delegate.setQuery(filter);
        }

        // return MongoRecordReader. Delegate is of type 'MongoInputSplit'
        return new MongoRecordReader(delegate);
    }

    DBObject getFilter(
      final JobConf conf, final Map<String, String> colToMongoNames) {
        String serializedExpr = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if (serializedExpr != null) {
            ExprNodeGenericFuncDesc expr =
              Utilities.deserializeExpression(serializedExpr);
            IndexPredicateAnalyzer analyzer =
              IndexPredicateAnalyzer.createAnalyzer(false);

            // Allow all column names.
            String columnNamesStr =
              conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
            String[] columnNames =
              StringUtils.split(columnNamesStr, StringUtils.COMMA);
            for (String colName : columnNames) {
                analyzer.allowColumnName(colName);
            }

            List<IndexSearchCondition> searchConditions =
              new LinkedList<IndexSearchCondition>();
            analyzer.analyzePredicate(expr, searchConditions);

            return getFilter(searchConditions, colToMongoNames);
        }
        return null;
    }

    DBObject getFilter(
      final List<IndexSearchCondition> searchConditions,
      final Map<String, String> colToMongoNames) {
        DBObject filter = new BasicDBObject();
        for (IndexSearchCondition isc : searchConditions) {
            String comparisonName = isc.getComparisonOp();
            Object constant = isc.getConstantDesc().getValue();
            String columnName = isc.getColumnDesc().getColumn();
            String mongoName = resolveMongoName(columnName, colToMongoNames);

            if (EQUAL_OP.equals(comparisonName)) {
                filter.put(mongoName, constant);
            } else {
                String mongoOp = MONGO_OPS.get(comparisonName);
                if (mongoOp != null) {
                    filter.put(
                      mongoName, new BasicDBObject(mongoOp, constant));
                } else {
                    // Log the fact that we don't support this operator.
                    // It's still ok to return a query; we'll just return a
                    // super set of the documents needed by Hive.
                    LOG.warn("unsupported operator type: " + comparisonName);
                }
            }
        }
        return filter;
    }

    DBObject getProjection(
      final JobConf conf, final Map<String, String> colToMongoNames) {
        boolean readAllCols =
          conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true);
        DBObject mongoProjection = null;
        if (!readAllCols) {
            String columnNamesStr =
              conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
            String[] columnNames =
              StringUtils.split(columnNamesStr, StringUtils.COMMA);
            boolean foundId = false;
            mongoProjection = new BasicDBObject();
            for (String col : columnNames) {
                String mapped = resolveMongoName(col, colToMongoNames);
                if ("_id".equals(mapped)) {
                    foundId = true;
                }
                mongoProjection.put(mapped, 1);
            }
            // Remove _id unless asked for explicitly.
            if (!foundId) {
                mongoProjection.put("_id", 0);
            }
        }
        return mongoProjection;
    }

    private Map<String, String> columnMapping(final JobConf conf) {
        String colMapString = conf.get(BSONSerDe.MONGO_COLS);
        if (null == colMapString) {
            return null;
        }
        BSONObject mappingBSON = (BSONObject) JSON.parse(colMapString);
        Map<String, String> mapping = new HashMap<String, String>();
        for (String key : mappingBSON.keySet()) {
            mapping.put(key.toLowerCase(), (String) mappingBSON.get(key));
        }
        return mapping;
    }

    private String resolveMongoName(
      final String colName, final Map<String, String> colNameMapping) {
        if (null == colNameMapping) {
            return colName;
        }
        String mapped = colNameMapping.get(colName);
        if (null == mapped) {
            return colName;
        }
        return mapped;
    }

    @Override
    public FileSplit[] getSplits(final JobConf conf, final int numSplits)
        throws IOException {
        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            final List<org.apache.hadoop.mapreduce.InputSplit> splits =
                splitterImpl.calculateSplits();
            InputSplit[] splitIns = splits.toArray(new InputSplit[splits.size()]);

            // wrap InputSplits in FileSplits so that 'getPath' 
            // doesn't produce an error (Hive bug)
            FileSplit[] wrappers = new FileSplit[splitIns.length];
            Path path = new Path(conf.get(MongoStorageHandler.TABLE_LOCATION));
            for (int i = 0; i < wrappers.length; i++) {
                wrappers[i] = new MongoHiveInputSplit(splitIns[i], path);
            }

            return wrappers;
        } catch (SplitFailedException spfe) {
            // split failed because no namespace found 
            // (so the corresponding collection doesn't exist)
            LOG.error(spfe.getMessage(), spfe);
            throw new IOException(spfe.getMessage(), spfe);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /*
     * MongoHiveInputSplit ->
     *  Used to wrap MongoInputSplits (as a delegate) to by-pass the Hive bug where
     *  'HiveInputSplit.getPath' is always called.
     */
    public static class MongoHiveInputSplit extends FileSplit {
        private InputSplit delegate;
        private Path path;

        MongoHiveInputSplit() {
            this(new MongoInputSplit());
        }

        MongoHiveInputSplit(final InputSplit delegate) {
            this(delegate, null);
        }

        MongoHiveInputSplit(final InputSplit delegate, final Path path) {
            super(path, 0, 0, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        public InputSplit getDelegate() {
            return delegate;
        }

        @Override
        public long getLength() {
            return 1L;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            delegate.write(out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            delegate.readFields(in);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}
