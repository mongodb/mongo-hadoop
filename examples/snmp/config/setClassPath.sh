CLASSPATH=.
for I in `ls /home/hadoopuser/hadoop20/**/*.jar ` ; do
   # echo $I
    CLASSPATH=$CLASSPATH:$I
done 
for J in `ls /home/hadoopuser/hadoop20/*.jar`; do
    echo $J
    CLASSPATH=$CLASSPATH:$J
done
 echo $CLASSPATH
 export CLASSPATH
