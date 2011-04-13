
#see http://www.mongodb.org/display/DOCS/A+Sample+Configuration+Session



DIR0=`dirname $0`
. $DIR0/set-env.sh
cd $DIR0

MONGOS_PID=`cat $DIR0/configdb/mongos.pid`
kill $MONGOS_PID && wait $MONGOS_PID

kill  `cat $DIR0/configdb/mongod.lock`
for I in 1 2 3 4; do
   kill `cat $DIR0/data$I/mongod.lock`
done

