The following test files are available in `src/test/resources`

    CREATE TABLE scores ( student int, name string, score int ) ROW FORMAT SERDE "com.mongodb.hadoop.hive.BSONSerde"
    STORED AS INPUTFORMAT "com.mongodb.hadoop.hive.input.BSONFileInputFormat"
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION "src/test/resources/testing/scores";

    LOAD DATA LOCAL INPATH "src/test/resources/scores.bson" INTO TABLE scores;

    CREATE TABLE books ( author array<string>, edition string, isbn string,   publicationYear int, publisher string, tags array<string>, title string ) ROW FORMAT SERDE "com.mongodb.hadoop.hive.BSONSerde"
    STORED AS INPUTFORMAT "com.mongodb.hadoop.hive.input.BSONFileInputFormat"
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION "src/test/resources/testing/scores";


    LOAD DATA LOCAL INPATH "src/test/resources/books.bson" INTO TABLE books;
