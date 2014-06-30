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
        if [[ "@HADOOP_VERSION@" == 2.* ]]
        then
            FORCE=-force
        fi
        @HADOOP_HOME@/bin/@BIN@ namenode -format ${FORCE} &> "@PROJECT_HOME@/logs/namenode-format.out"
    fi
    
    for LOG in @PROJECT_HOME@/logs/*.log
    do
        > $LOG
    done
        
    startService @BIN@ namenode
    sleep 5
    startService @BIN@ datanode
    if [[ "@HADOOP_VERSION@" != 1.* ]]
    then
        startService yarn resourcemanager
        startService yarn nodemanager
    else
        startService hadoop jobtracker
        startService hadoop tasktracker
    fi
    
    if [[ "@HADOOP_VERSION@" == *cdh4* ]]
    then 
        @HADOOP_HOME@/bin/hadoop fs -mkdir @HIVE_HOME@/lib
        @HADOOP_HOME@/bin/hadoop fs -put @HIVE_HOME@/lib/hive-builtins-*.jar @HIVE_HOME@/lib
        sleep 5
    fi

    echo Starting hiveserver
    @HIVE_HOME@/bin/hive --service hiveserver &> "@PROJECT_HOME@/logs/hiveserver.log" &
}

stopAll() {
    stopService NodeManager node manager
    stopService ResourceManager resource manager
    stopService DataNode data node
    stopService JobTracker job tracker
    stopService TaskTracker task tracker
    stopService NameNode name node
    stopService RunJar hive server
}
shutdown() {
    stopAll
        
    KILL_SIGNAL=KILL

    stopAll
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
