#!/bin/bash
 
stopService() {
    SERVICE=$1
    shift
    PID_PATH=@HADOOP_HOME@/${SERVICE}.pid
    if [ -f ${PID_PATH} ]
    then
        echo Shutting down $*
        kill `cat ${PID_PATH}`
        rm ${PID_PATH}
    fi
}

startService() {
    BIN=$1
    SERVICE=$2
    echo Starting ${SERVICE}
    @HADOOP_HOME@/bin/${BIN} ${SERVICE} &> @HADOOP_HOME@/${SERVICE}.log &
    echo $! > @HADOOP_HOME@/${SERVICE}.pid
}

start() {
    ./gradlew copyFiles
    startService hdfs namenode
    sleep 3
    startService hdfs datanode
    startService yarn resourcemanager
    startService yarn nodemanager
    
    echo Starting hiveserver
    export HADOOP_HOME=@HADOOP_HOME@
    @HIVE_HOME@/bin/hive --service hiveserver &> @HIVE_HOME@/hiveserver.log &
    echo $! > @HIVE_HOME@/hiveserver.pid
}

shutdown() {
    stopService nodemanager node manager
    stopService resourcemanager resource manager
    stopService datanode data node
    stopService namenode name node
    
    PID_PATH=@HIVE_HOME@/hiveserver.pid
    if [ -f ${PID_PATH} ]
    then
        echo Shutting down hive server
        kill `cat ${PID_PATH}`
        rm ${PID_PATH}
    fi
}

CMD=$1

if [ "$2" == "-format" ]
then
    shutdown
    sleep 1
    rm -rf @HADOOP_BINARIES@/tmpdir/
    @HADOOP_HOME@/bin/hdfs namenode -format -force &> @HADOOP_HOME@/namenode-format.log
fi

if [ "$CMD" == "shutdown" ]
then
    shutdown
else
    start
fi