#!/bin/bash
 
stopService() {
    SERVICE=$1
    shift
    if [ -f @BIN_PATH@/${SERVICE}.pid ]
    then
        echo Shutting down $*
        kill `cat @BIN_PATH@/${SERVICE}.pid`
        rm @BIN_PATH@/${SERVICE}.pid
	fi
}

startService() {
    BIN=$1
    SERVICE=$2
    echo Starting ${SERVICE}
    @HADOOP_HOME@/bin/${BIN} ${SERVICE} &> @BIN_PATH@/${SERVICE}.log &
    echo $! > @BIN_PATH@/${SERVICE}.pid
    sleep 3
}

start() {
    startService hdfs namenode
    startService hdfs datanode
    startService yarn resourcemanager
    startService yarn nodemanager
    export HADOOP_HOME=@HADOOP_HOME@
    @HIVE_HOME@/bin/hive --service hiveserver &> @BIN_PATH@/hiveserver.log &
    echo $! > @BIN_PATH@/hiveserver.pid
}

shutdown() {
	stopService nodemanager node manager
	stopService resourcemanager resource manager
	stopService datanode data node
	stopService namenode name node
	stopService hiveserver hive server
}

CMD=$1

if [ "$2" == "-format" ]
then
    shutdown
    sleep 1
    rm -r @HADOOP_BINARIES@/tmpdir/
    @HADOOP_HOME@/bin/hdfs namenode -format -force 1> /dev/null
fi

if [ "$CMD" == "shutdown" ]
then
    shutdown
else
    start
fi