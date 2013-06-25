



public class MongoInputSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
    protected MongoURI inputURI;
    protected BSONObject fields;
    protected BSONObject query;
    protected BSONObject sort;
    protected BSONObject min;
    protected BSONObject max;
    protected boolean notimeout = false;
    protected transient DBCursor cursor;

    protected static transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
    protected static transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();

    public class MongoInputSplit(){}

    public void setInputURI(MongoURI inputURI){
        this.inputURI = inputURI;
    }

    public MongoURI getInputURI(){
        return this.inputURI;
    }

    public BSONObject getFields(){//{{{
        return this.fields;
    }

    public void setFields(BSONObject fields){
        this.fields = fields;
    }//}}}

    public BSONObject getQuery(){//{{{
        return this.query;
    }

    public void setQuery(BSONObject query){
        this.query = query;
    }//}}}

    public BSONObject getSort(){//{{{
        return this.sort;
    }

    public void setSort(BSONObject sort){
        this.sort = sort;
    }//}}}

    public BSONObject getMin(){//{{{
        return this.min;
    }

    public void setMin(BSONObject min){
        this.min = min;
    }//}}}

    public BSONObject getMax(){//{{{
        return this.max;
    }

    public void setMax(BSONObject max){
        this.max = max;
    }//}}}

    public boolean getNoTimeout(){//{{{
        return this.notimeout;
    }

    public void setNoTimeout(boolean notimeout){
        this.notimeout = notimeout;
    }//}}}

    @Override
    public void write(final DataOutput out) throws IOException{
        BSONObject spec = BasicDBObjectBuilder.start().
                           add( "inputURI", getInputURI().toString()).
                           add( "fields", getFields()).
                           add( "query", getQuery()).
                           add( "sort", getSort()).                                              
                           add( "min", getMin()).
                           add( "max", getMax()).
                           add( "notimeout", getNoTimeout()).get();
        byte[] buf = _bsonEncoder.encode(spec);
        out.write(buf);
    }

    @Override
    public void readFields(final DataInput in) throws IOException{
        BSONCallback cb = new BasicBSONCallback();
        BSONObject spec;
        byte[] l = new byte[4];
        in.readFully( l );
        int dataLen = org.bson.io.Bits.readInt( l );
        byte[] data = new byte[dataLen + 4];
        System.arraycopy( l, 0, data, 0, 4 );
        in.readFully( data, 4, dataLen - 4 );
        _bsonDecoder.decode( data, cb );
        spec = (BSONObject)cb.get();
        setInputURI(new MongoURI((String)spec.get("inputURI")));
        setFields(new BasicBSONObject(((BSONObject)spec.get("fields")).toMap()));
        setQuery(new BasicBSONObject(((BSONObject)spec.get("query")).toMap()));
        setSort(new BasicBSONObject(((BSONObject)spec.get("sort")).toMap()));
        setMin(new BasicBSONObject(((BSONObject)spec.get("min")).toMap()));
        setMax(new BasicBSONObject(((BSONObject)spec.get("max")).toMap()));
        setNoTimeout((Boolean)spec.get("notimeout"));
    }

    public DBCursor getCursor(){
        if(this.cursor == null){
            this.cursor = MongoConfigUtil.getCollection(this.inputURI).find(this.query, this.fields).sort(this.sort);
            if (this.notimeout) this.cursor.setOptions( Bytes.QUERYOPTION_NOTIMEOUT );
            if (this.min != null) this.cursor.addSpecial("$min", this.min);
            if (this.max != null) this.cursor.addSpecial("$max", this.max);
        }
        return this.cursor;
    }

    @Override
    public String toString(){
        return "MongoInputSplit{URI=" + this.inputURI.toString()
             + ", min=" + this.min + ", max=" + this.max 
             + ", query=" + this.query
             + ", sort=" + this.sort
             + ", fields=" + this.fields
             + ", notimeout=" + this.notimeout + '}' ;
    }

    @Override
    public int hashCode(){
        int result = this.inputURI != null ? this.inputURI.hashCode() : 0;
        result = 31 * result + ( this.query != null ? this.query.hashCode() : 0 );
        result = 31 * result + ( this.fields != null ? this.fields.hashCode() : 0 );
        result = 31 * result + ( this.max != null ? this.max.hashCode() : 0 );
        result = 31 * result + ( this.min != null ? this.min.hashCode() : 0 );
        result = 31 * result + ( this.sort != null ? this.sort.hashCode() : 0 );
        result = 31 * result + ( this.notimeout ? 1 : 0 );
        return result;
    }

    @Override
    public boolean equals(Object o){
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;
        MongoInputSplit that = (MongoInputSplit) o;

        if ( getNoTimeout() != that.getNoTimeout() ) return false;
        if ( getFields() != null ? !getFields().equals( that.getFields() ) : that.getFields() != null ) return false;
        if ( getInputURI() != null ? !getInputURI().equals( that.getInputURI() ) : that.getInputURI() != null ) return false;
        if ( getQuery() != null ? !_querySpec.equals( that._querySpec ) : that.getQuery() != null ) return false;
        if ( getSort() != null ? !getSort().equals( that.getSort() ) : that.getSort() != null ) return false;
        if ( getMax() != null ? !getMax().equals( that.getMax() ) : that.getMax() != null ) return false;
        if ( getMin() != null ? !getMin.equals( that.getMin() ) : that.getMin() != null ) return false;
        return true;
    }

}
