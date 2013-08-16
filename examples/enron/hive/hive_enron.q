-- Hive doesn't allow hyphens in field names

-- This hive script takes in the emails from Enron and
-- counts the numbers exchanged between each pair of people

-- Get the headers struct, which contains the "from" and "to".
-- except the words "from", "to", and "date" are reserved in Hive 
CREATE TABLE raw(
    headers STRUCT<
        x_cc:STRING, 
        anotherfrom:STRING,
        subject : STRING,
        x_folder : STRING,
        content_transfer_encoding : STRING,
        x_bcc : STRING,
        anotherto : STRING,
        x_origin : STRING,
        x_filename : STRING,
        x_from : STRING,
        anotherdate : STRING,
        x_to : STRING,
        message_id : STRING,
        content_type : STRING,
        mime_version : STRING
    >
)
STORED BY 'com.mongodb.hadoop.hive.BSONStorageHandler'
LOCATION '${INPUT}/messagesNew/';


CREATE TABLE send_recip (
    f STRING,
    t_array ARRAY<STRING>
);

-- Strip the white space from the "anotherto" string
-- Then split the comma delimited string into an array of strings
INSERT OVERWRITE TABLE send_recip 
SELECT 
    headers.anotherfrom AS f,
    split(headers.anotherto, "\\s*,\\s*") 
        AS t_array
FROM raw
WHERE headers IS NOT NULL 
    AND headers.anotherto IS NOT NULL;


CREATE TABLE send_recip_explode (
    f STRING, 
    t STRING,
    num INT
);

-- Explode the array so that every element in the array gets it
-- own row. Then group by the unique "f" and "t" pair
-- to find the number of emails between the sender and receiver
INSERT OVERWRITE TABLE send_recip_explode
SELECT 
    f, 
    t, 
    count(1) AS num
FROM send_recip
    LATERAL VIEW explode(t_array) tmpTable AS t
GROUP BY f, t;


CREATE TABLE send_recip_counted (
    id STRUCT<
        t : STRING, 
        f : STRING
    >,
    count INT
)
STORED BY 'com.mongodb.hadoop.hive.BSONStorageHandler'
WITH SERDEPROPERTIES ("mongo.columns.mapping"="_id,count")
LOCATION '${OUTPUT}/outs/';

-- Final output with the correct format
INSERT INTO TABLE send_recip_counted
SELECT 
    named_struct('t', t, 'f', f) AS id,
    num AS count
FROM send_recip_explode;
