



DIR0=`dirname $0`
. $DIR0/set-env.sh
cd $DIR0

echo $0: setting up replica set
echo
mongo 127.0.0.1:20003 setup-replica-set.js

#For some reason if the replica set isn't up and running
#the command to add it as a shard will fail.
echo $0: sleeping 5 seconds to allow replica set to sync
sleep 5

echo
echo $0: setting up setting up shards

mongo 127.0.0.1:30000/admin setup-shards.js

if ! [ -f big.txt ] ; then
wget http://norvig.com/big.txt
fi

#either of these two ways should work. One loads using default _id's, the other one loads
#setting _id to be a unique number
if /bin/false ; then
    perl -e 'while (<>) {  s/\"/\\"/g ; print }' < big.txt | sed 's/.*/  db.lines.insert({"line": "&"});/' > big.js

    #load data into database
    mongo  127.0.0.1:30000 big.js
else

    TMPFILE=`mktemp`
    echo num = 1 >> $TMPFILE
    perl -e 'while (<>) {  s/\"/\\"/g ; print }' < big.txt | sed 's/.*/  db.lines.insert({"num": num++, "line": "&"});/' >> $TMPFILE

    mv $TMPFILE $TMPFILE.js

    echo running $TMPFILE.js

    #load data into database
    mongo  127.0.0.1:30000/test $TMPFILE.js

    rm $TMPFILE.js
fi
