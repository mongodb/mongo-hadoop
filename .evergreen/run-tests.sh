#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_BINARIES        The location of the MongoDB binaries, e.g. /usr/local/bin
#       HADOOP_VERSION          Sets the version of Hadoop to be used.
#       AUTH                    Set to enable authentication. Values are: "auth" / "noauth" (default)
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8"

MONGODB_BINARIES=${MONGODB_BINARIES:-}
AUTH=${AUTH:-noauth}
JDK=${JDK:-jdk}
PROJECT_DIRECTORY=${PROJECT_DIRECTORY:-}

export HADOOP_VERSION=${HADOOP_VERSION:-2.7.2}
export HADOOP_PREFIX=$PROJECT_DIRECTORY/hadoop-binaries/hadoop-$HADOOP_VERSION
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_USER_CLASSPATH_FIRST=true
export HIVE_HOME=$PROJECT_DIRECTORY/hadoop-binaries/apache-hive-1.2.1-bin

export JAVA_HOME="/opt/java/${JDK}"

./gradlew -version
./gradlew -Dmongodb_bin_dir=${MONGODB_BINARIES} -Dmongodb_option=${AUTH} -DHADOOP_VERSION=${HADOOP_VERSION} --stacktrace jar testsJar test cleanHadoop