#customize this for your environment


DIR0=`dirname $0`
#cd $DIR0
export PATH=$HOME/work/mongo/mongodb-src:$PATH


#In theory I shouldn't have to do this. What should happend
#is that I set HADOOP_CLASSPATH=. and run the hadoop command.
#However that does not work for some reason.

CLASSPATH=$DIR0
LIBDIR=$HOME/mongo/hadoop/lib
for I in `ls $LIBDIR| grep jar ` ; do
   CLASSPATH=$CLASSPATH:$LIBDIR/$I
done 
LIBDIR=$HOME/mongo/hadoop/
for I in `ls $LIBDIR| grep jar ` ; do
   CLASSPATH=$CLASSPATH:$LIBDIR/$I
done 

 export CLASSPATH
