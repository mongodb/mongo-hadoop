package com.mongodb.hadoop.hive;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import com.mongodb.DBCollection;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;

/*
 * Used to sync documents in some MongoDB collection with
 * rows in a Hive table
 */
public class MongoStorageHandler extends DefaultStorageHandler {
	// stores the location of the collection
	public static final String MONGO_URI = "mongo.uri";
	// stores the 1-to-1 mapping of MongoDB fields to hive columns
	public static final String MONGO_COLS = "mongo.columns.mapping";
	// get location of where meta-data is stored about the mongo collection
	public static final String TABLE_LOCATION = "location";
	
	public MongoStorageHandler() { }

	@Override
	public Class<? extends InputFormat<?,?>> getInputFormatClass() {
		return HiveMongoInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new MongoHiveMetaHook();
	}

	@Override
	public Class<? extends OutputFormat<?,?>> getOutputFormatClass() {
		return HiveMongoOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return MongoSerDe.class;
	}
	
	/*
	 * HiveMetaHook used to define events triggered when a hive table is
	 * created and when a hive table is dropped.
	 */
	public class MongoHiveMetaHook implements HiveMetaHook {
		
		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			Map<String, String> tblParams = tbl.getParameters();
			if (!tblParams.containsKey(MONGO_URI)) {
				throw new MetaException("You must specify 'mongo.uri' in TBLPROPERTIES");
			}
		}
		
		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
		}
		
		@Override
		public void preDropTable(Table tbl) throws MetaException {
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
			boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

			if (deleteData && !isExternal) {
				Map<String, String> tblParams = tbl.getParameters();
				if (tblParams.containsKey(MONGO_URI)) {
					String mongoURIStr = tblParams.get(MONGO_URI);
					DBCollection coll = MongoConfigUtil.getCollection(new MongoURI(mongoURIStr));
					coll.drop();
				} else {
					throw new MetaException("No 'mongo.uri' property found. Collection not dropped.");
				}
			}
		}
		
		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
		}
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		copyJobProperties(properties, jobProperties);
	}
	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		copyJobProperties(properties, jobProperties);
	}
	
	/*
	 * Helper function to copy properties
	 */
	private void copyJobProperties(Properties from, Map<String, String> to) {
		for (Entry<Object, Object> e : from.entrySet()) {
			to.put((String)e.getKey(), (String)e.getValue());
		}
		
		if (to.containsKey(MONGO_URI)) {
			String mongoURIStr = to.get(MONGO_URI);
			to.put(MongoConfigUtil.INPUT_URI,  mongoURIStr);
			to.put(MongoConfigUtil.OUTPUT_URI, mongoURIStr);
		}
	}
}
