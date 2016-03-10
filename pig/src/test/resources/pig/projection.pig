data =
    LOAD 'mongodb://localhost:27017/mongo_hadoop.projection_test'
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray,i:int,d:[]', 'id');

-- Pig only pushes projections with subfields when the outer field is a map (d).
projected =
    FOREACH data
    GENERATE $1 AS age, d#'s' AS name, d#'k' AS ssn;

STORE projected INTO 'test_results';
