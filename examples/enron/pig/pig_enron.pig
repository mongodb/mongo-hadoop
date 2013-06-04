--Change these jar locations to point to the correct locations/version on your system.
REGISTER  /Users/mike/Downloads/mongo-2.10.1.jar;
REGISTER ../core/target/mongo-hadoop-core_cdh4.3.0-1.1.0.jar
REGISTER ../pig/target/mongo-hadoop-pig_cdh4.3.0-1.1.0.jar


raw = LOAD 'file:///tmp/enron_mail/messages.bson' using com.mongodb.hadoop.pig.BSONLoader('','headers:[]') ; 
send_recip = FOREACH raw GENERATE $0#'From' as from, $0#'To' as to;
send_recip_filtered = FILTER send_recip BY to IS NOT NULL;
send_recip_split = FOREACH send_recip_filtered GENERATE from as from, FLATTEN(TOKENIZE(to)) as to;
send_recip_split_trimmed = FOREACH send_recip_split GENERATE from as from, TRIM(to) as to;
send_recip_grouped = GROUP send_recip_split_trimmed BY (from, to);
send_recip_counted = FOREACH send_recip_grouped GENERATE group, COUNT($1) as count;
STORE send_recip_counted INTO 'file:///tmp/enron_result.bson' using com.mongodb.hadoop.pig.BSONStorage;
