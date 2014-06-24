#! /bin/sh

alias g="./gradlew --daemon"

HV=$1
VERSIONS=(0.23 1.0 1.1 2.2 2.3 2.4 cdh4 cdh5)
#VERSIONS=(0.23 cdh4 cdh5 2.2 2.3 2.4)

function choose() {
	if [ -z "${HV}" ]
	then
		echo Choose a hadoop version
		select HV in ${VERSIONS[@]}
		do
			break
		done
	fi
}

function build() {
	g clean jar testJar -PclusterVersion=${HV} 2>&1 | tee test-${HV}.out
}

function browser() {
	while [ "$1" ]
	do
		[ -f $1 ] && open $1
		shift
	done
}

function run() {
	echo g configureCluster test -PclusterVersion=${HV} 2>&1 | tee -a test-${HV}.out
	./build/hadoop-${HV}.sh shutdown
	if [ "`grep -i failed /Users/jlee/dev/mongo-hadoop/core/build/reports/tests/index.html 2> /dev/null`" -o \
				"`grep -i failed /Users/jlee/dev/mongo-hadoop/hive/build/reports/tests/index.html 2> /dev/null`" ]
	then
		echo "********** Found failing tests.  Exiting."
		browser /Users/jlee/dev/mongo-hadoop/core/build/reports/tests/index.html \
			/Users/jlee/dev/mongo-hadoop/hive/build/reports/tests/index.html
		exit
	fi
}

if [ "$1" == "all" ]
then
	for HV in ${VERSIONS[@]}
	do
		> test-${HV}.out
	done
	for HV in ${VERSIONS[@]}
	do
		build
		run
	done
else
	choose
	build
	run
fi
