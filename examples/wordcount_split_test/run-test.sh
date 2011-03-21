

export HADOOP_CLASSPATH=
cd `dirname $0`
jar -cf wc-split-test.jar *.class

hadoop jar ./wc-split-test.jar  WordCountSplitTest $*

#  -D mapred.job.tracker=local 


