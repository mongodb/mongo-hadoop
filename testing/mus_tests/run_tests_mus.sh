#! /bin/sh
# script to setup and run pig scripts
# to test implementation of
# MongoUpdateStorage

# drop 'test.update_mus' collection if it already exists
mongo test --eval "db.update_mus.drop()" &

# 1. MongoUpdateStorage should insert 
#    (first, last, cars) 
pig -x local update_simple_mus.pig

# 2. MongoUpdateStorage should "double" the cars (2 per unique car)
#    but should leave the other attributes unchanged
pig -x local update_simple_mus.pig

# 3. MongoUpdateStorage should "insert" an age for each person
pig -x local update_age_mus.pig

# 4. MongoUpdateStorage should "increment" the age of every person by 1
pig -x local update_age_incr_mus.pig

# 5. MongoUpdateStorage should "double" the age of every person with lastname "Alabi".
#    Uses 'multi' flag.
pig -x local update_age_alabis_mus.pig

OUTPUT_FILE="update_mus.csv"
EXP_OUTPUT_FILE="expected_update_mus.csv"

# export to file
mongoexport --db test --collection update_mus --fields first,last,age,cars --csv --out $OUTPUT_FILE

echo "\n!!! output stored in $OUTPUT_FILE !!!\n"

cat $OUTPUT_FILE

if diff -w $OUTPUT_FILE $EXP_OUTPUT_FILE > /dev/null; then
    echo "\n!!! Output same as expected! Test passed!"
else
   echo "\n!!! Output NOT the same as expected! Test failed!"
fi

