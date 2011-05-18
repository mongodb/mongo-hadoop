
#see http://www.mongodb.org/display/DOCS/A+Sample+Configuration+Session


DIR0=`dirname $0`
. $DIR0/set-env.sh
cd $DIR0


 mkdir -p logs
 mkdir -p $DIR0/configdb
 mongod --dbpath  $DIR0/configdb --rest --port 20010 > logs/configdb.log &
if [ $? -gt 0 ] ; then
   exit
fi

for I in 1 2 3 4 ; do
   mkdir -p $DIR0/data$I
   if [ $I -gt 2 ] ; then
     #put 3 and 4 into a replica set
     mongod --replSet foo --dbpath `dirname $0`/data$I --rest --port 2001$I > logs/db-$I.log &
   else
     mongod --dbpath `dirname $0`/data$I --rest --port 2001$I > logs/db-$I.log &
   fi
#PID_DB_$I=$!
done


sleep 3
mongos --port 30010  --pidfilepath $DIR0/configdb/mongos.pid  --chunkSize 1 --configdb localhost:20010 > logs/mongos.log &
