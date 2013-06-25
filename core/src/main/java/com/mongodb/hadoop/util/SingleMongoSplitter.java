public class SingleMongoSplitter extends MongoSplitter{

    //Create a single split which consists of a single 
    //a query over the entire collection.

    @Override
    public List<InputSplit> calculateSplits(){
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        MongoInputSplit mongoSplit = new MongoInputSplit();
        mongoSplit.setInputURI(MongoConfigUtil.getInputURI(this.conf));
        mongoSplit.setQuery(MongoConfigUtil.getQuery(this.conf));
        mongoSplit.setFields(MongoConfigUtil.getFields(this.conf));
        mongoSplit.setSort(MongoConfigUtil.getSort(this.conf));
        mongoSplit.setNoTimeout(MongoConfigUtil.isNoTimeout(this.conf));
        mongoSplit.setMin(null);
        mongoSplit.setMax(null);
        splits.add(mongoSplit);
        return splits;
    }

}
