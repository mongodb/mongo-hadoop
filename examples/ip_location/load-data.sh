

if [ -z "$1" ] ; then
    echo format: $0 logfile
    exit 1
fi


DIR0=`dirname $0`
. $DIR0/set-env.sh
cd $DIR0

export LIBDIR=$DIR0/../../lib
CLASSPATH=../../mongo-hadoop.jar 
#echo jars: `ls $LIBDIR| grep jar`
for I in `ls $LIBDIR| grep jar ` ; do
   CLASSPATH=$CLASSPATH:$LIBDIR/$I
done 

export CLASSPATH=$CLASSPATH:$DIR0/build

#echo CLASSPATH is $CLASSPATH
java ApacheLogFileReader  mongodb://127.0.0.1:30010/test.weblog $1

