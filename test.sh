#! /bin/sh

alias g="./gradlew --daemon"

HV=$1

if [ -z "${HV}" ]
then
	echo Choose a hadoop version
	select HV in 0.23 1.0 1.1 cdh4 cdh5 2.2 2.3 2.4 
	do
		break
	done
fi

./bin/hadoop-all.sh shutdown
g clean jar -Phadoop_version=${HV}
./bin/hadoop-all.sh start -format
g test -Phadoop_version=${HV} 2>&1 | tee test-${HV}.out
./bin/hadoop-all.sh shutdown
