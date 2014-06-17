#!/bin/bash
 
stopService() {
    SERVICE=$1
    shift
    PID=`jps | grep ${SERVICE} | cut -d' ' -f1`
    if [ "${PID}" ]
    then
        echo Shutting down $*
        kill -s ${KILL_SIGNAL-TERM} ${PID}
    fi
}

startService() {
    BIN=$1
    SERVICE=$2
    echo Starting ${SERVICE}
    @HADOOP_HOME@/bin/${BIN} ${SERVICE} &> "@PROJECT_HOME@/logs/${SERVICE}.log" &
}

start() {
    shutdown
    
    if [ "$1" == "-format" ]
    then
        rm -rf @HADOOP_BINARIES@/tmpdir/
        if [[ "@HADOOP_VERSION@" != 0.23* ]]
        then
            FORCE=-force
        fi
        @HADOOP_HOME@/bin/hdfs namenode -format ${FORCE} &> "@PROJECT_HOME@/logs/namenode-format.out"
    fi
    
    startService hdfs namenode
    sleep 5
    startService hdfs datanode
    startService yarn resourcemanager
    startService yarn nodemanager
    
    echo Starting hiveserver
    export HADOOP_HOME=@HADOOP_HOME@
    if [[ "@HADOOP_VERSION@" == *cdh* ]]
    then
        export MAPRED_DIR=@HADOOP_HOME@/share/hadoop/mapreduce2
        if [[ "@HADOOP_VERSION@" == *cdh4* ]]
        then 
            @HADOOP_HOME@/bin/hadoop fs -mkdir @HIVE_HOME@/lib
            @HADOOP_HOME@/bin/hadoop fs -put @HIVE_HOME@/lib/hive-builtins-*.jar @HIVE_HOME@/lib
        fi
    fi
    @HIVE_HOME@/bin/hive --service hiveserver &> "@PROJECT_HOME@/logs/hiveserver.log" &
}

shutdown() {
    stopService NodeManager node manager
    stopService ResourceManager resource manager
    stopService DataNode data node
    stopService NameNode name node
    stopService RunJar hive server
    
    KILL_SIGNAL=KILL
    stopService NodeManager node manager
    stopService ResourceManager resource manager
    stopService DataNode data node
    stopService NameNode name node
    stopService RunJar hive server
}


if [ "$1" == "shutdown" ]
then
    shutdown
else
    if [ "$1" == "start" ]
    then
        shift
    fi
    start $*
fi
