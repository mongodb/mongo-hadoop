
CREATE TABLE scores ( student int, name string, score int ) ROW FORMAT SERDE "com.mongodb.hadoop.hive.BSONSerde"
STORED AS INPUTFORMAT "com.mongodb.hadoop.BSONFileInputFormat"
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH "<FULL_WORKING_DIR>/src/test/resources/scores/scores.bson" INTO TABLE scores;

CREATE TABLE books ( author array<string>, edition string, isbn string,   publicationYear int, publisher string, tags array<string>, title string ) ROW FORMAT SERDE "com.mongodb.hadoop.hive.BSONSerde"
STORED AS INPUTFORMAT "com.mongodb.hadoop.hive.input.BSONFileInputFormat"
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH "<FULL_WORKING_DIR>/src/test/resources/books/books.bson" INTO TABLE books;
