package com.mongodb.hadoop.util;

import com.mongodb.*;
import com.mongodb.hadoop.input.MongoInputSplit;
import java.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.*;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;


/* This class is an implementation of MongoSplitter which
 * calculates a list of splits on a single collection
 * by running the MongoDB internal command "splitVector",
 * which generates a list of index boundary pairs, each 
 * containing an approximate amount of data depending on the
 * max chunk size used, and converting those index boundaries
 * into splits.
 *
 * This splitter is the default implementation used for any
 * collection which is not sharded.
 *
 */
public class StandaloneMongoSplitter extends MongoCollectionSplitter{

    private static final Log log = LogFactory.getLog( StandaloneMongoSplitter.class );

    final DBObject splitKey;
    final int splitSize;

    public StandaloneMongoSplitter(Configuration conf, MongoURI inputURI, DBObject splitKey, int splitSize){
        super(conf, inputURI);
        this.splitKey = splitKey;
        this.splitSize = splitSize;
    }

    // Generate one split per chunk.
    @Override
    public List<InputSplit> calculateSplits() throws SplitFailedException{
        this.init();

        final ArrayList<InputSplit> returnVal = new ArrayList<InputSplit>();
        final String ns = this.inputCollection.getFullName();

        log.info("Running splitvector to check splits against " + this.inputURI);
        final DBObject cmd = BasicDBObjectBuilder.start("splitVector", ns).
                                          add( "keyPattern", this.splitKey ).
                                          add( "force", false ). // force:True is misbehaving it seems
                                          add( "maxChunkSize", this.splitSize ).get();
        
        CommandResult data;
        if(this.authDB == null){
            data = this.inputCollection.getDB().getSisterDB("admin").command( cmd );
        }else{
            data = this.authDB.command( cmd );
        }

        if ( data.containsField( "$err" ) ){
            throw new SplitFailedException( "Error calculating splits: " + data );
        } else if ( !((Double)data.get("ok")).equals(1.0) )
            throw new SplitFailedException( "Unable to calculate input splits: " + ( (String) data.get( "errmsg" ) ) );
        
        // Comes in a format where "min" and "max" are implicit
        // and each entry is just a boundary key; not ranged
        BasicDBList splitData = (BasicDBList) data.get( "splitKeys" );

        if (splitData.size() <= 1) {
            if (splitData.size() < 1)
                log.warn( "WARNING: No Input Splits were calculated by the split code. " +
                          "Proceeding with a *single* split. " + 
                          "Data may be too small, try lowering 'mongo.input.split_size' " +
                          "if this is undesirable." );
            // no splits really. Just do the whole thing data is likely small
            MongoInputSplit oneBigSplit = createSplitFromBounds((BasicDBObject)null, (BasicDBObject)null);
            returnVal.add(oneBigSplit);
        } else {
            //First split, with empty lower boundary
            BasicDBObject lastKey = (BasicDBObject) splitData.get( 0 );
            MongoInputSplit firstSplit = createSplitFromBounds( (BasicDBObject)null, lastKey);
            returnVal.add(firstSplit);

            for (int i = 1; i < splitData.size(); i++ ) {
                final BasicDBObject _tKey = (BasicDBObject)splitData.get(i);
                MongoInputSplit split = createSplitFromBounds(lastKey, _tKey);
                lastKey = _tKey;
            }

            //Last max split, with empty upper boundary
            MongoInputSplit lastSplit = createSplitFromBounds(lastKey, (BasicDBObject)null);
            returnVal.add(lastSplit);
        }
        return returnVal;
    }

}
