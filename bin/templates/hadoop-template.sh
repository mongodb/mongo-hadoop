#!/bin/bash
 
stopService() {
    SERVICE=$1
    shift
    PID=`jps | grep ${SERVICE} | cut -d' ' -f1`
    if [ "${PID}" ]
    then
        echo kill -s ${KILL_SIGNAL-TERM} ${PID}
        kill -s ${KILL_SIGNAL-TERM} ${PID}
    fi
}

startService() {
    BIN=$1
    SERVICE=$2
    echo "@HADOOP_HOME@/bin/${BIN} ${SERVICE} &> \"@PROJECT_HOME@/logs/${SERVICE}.log\""
    @HADOOP_HOME@/bin/${BIN} ${SERVICE} &> "@PROJECT_HOME@/logs/${SERVICE}.log" &
}

start() {
    shutdown
    
    unset HADOOP_PREFIX
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
    if [ "${JENKINS_URL}" ]
    then
        echo Sleep 30s to let the namenode settle
        sleep 30
    else
        sleep 5
    fi
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
        echo @HADOOP_HOME@/bin/hadoop fs -mkdir @HIVE_HOME@/lib
        @HADOOP_HOME@/bin/hadoop fs -mkdir @HIVE_HOME@/lib
        echo @HADOOP_HOME@/bin/hadoop fs -put @HIVE_HOME@/lib/hive-builtins-*.jar @HIVE_HOME@/lib
        @HADOOP_HOME@/bin/hadoop fs -put @HIVE_HOME@/lib/hive-builtins-*.jar @HIVE_HOME@/lib
        sleep 5
    fi

    export HADOOP_HOME=@HADOOP_HOME@
    if [[ "@HADOOP_VERSION@" == *cdh* ]]
    then
        export MAPRED_DIR=@HADOOP_HOME@/share/hadoop/mapreduce2
    fi
    echo @HIVE_HOME@/bin/hive --service hiveserver &> "@PROJECT_HOME@/logs/hiveserver.log" &
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
