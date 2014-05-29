#!/bin/bash
 
stopService() {
    SERVICE=$1
    shift
    PID=`jps | grep ${SERVICE} | cut -d' ' -f1`
    if [ "${PID}" ]
    then
        echo Shutting down $*
        kill ${PID}
    fi
}

startService() {
    BIN=$1
    SERVICE=$2
    echo Starting ${SERVICE}
    @HADOOP_HOME@/bin/${BIN} ${SERVICE} &> @PROJECT_HOME@/logs/${SERVICE}.log &
}

start() {
    shutdown
    ./gradlew configureCluster -Phadoop_version=@HADOOP_VERSION@
    
    if [ "$1" == "-format" ]
    then
        sleep 1
        rm -rf @HADOOP_BINARIES@/tmpdir/
        if [ "@HADOOP_VERSION@" != "0.23" ]
        then
            FORCE=-force
        fi
        @HADOOP_HOME@/bin/hdfs namenode -format ${FORCE} &> @PROJECT_HOME@/logs/namenode-format.out
    fi
    
    startService hdfs namenode
    startService hdfs datanode
    startService yarn resourcemanager
    startService yarn nodemanager
    
    echo Starting hiveserver
    export HADOOP_HOME=@HADOOP_HOME@
    if [[ "@HADOOP_VERSION@" == cdh* ]]
    then
        echo CDH found.  setting MAPRED_DIR
        export MAPRED_DIR=share/hadoop/mapreduce2
    fi
    @HIVE_HOME@/bin/hive --service hiveserver &> @PROJECT_HOME@/logs/hiveserver.log &
}

shutdown() {
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
