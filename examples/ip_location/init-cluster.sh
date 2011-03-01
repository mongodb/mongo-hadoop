



DIR0=`dirname $0`
. $DIR0/set-env.sh
cd $DIR0

echo $0: setting up replica set
echo
mongo 127.0.0.1:20013 setup-replica-set.js

#For some reason if the replica set isn't up and running
#the command to add it as a shard will fail.
echo $0: sleeping 5 seconds to allow replica set to sync
sleep 5

echo
echo $0: setting up setting up shards

mongo 127.0.0.1:30010/admin setup-shards.js

