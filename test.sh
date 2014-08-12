#! /bin/sh

alias g="./gradlew --daemon"

VERSIONS=(0.23 1.0 1.1 2.2 2.3 2.4 cdh4 cdh5)
OPTS=test
#VERSIONS=(0.23 cdh4 cdh5 2.2 2.3 2.4)

while [ "$1" ]
do
	if [[ " ${VERSIONS[*]} " == *" $1 "* ]]
	then
		HV=$1
	else 
		case $1 in
			"examples")
				OPTS="historicalYield sensorData enronEmails"
				;;
			"all")
				HV="all"
				;;
		esac
	fi
	shift
done

echo Running \"$OPTS\" against \"${HV}\"

function choose() {
	if [ -z "${HV}" ]
	then
		echo Choose a hadoop version
		select HV in ${VERSIONS[@]}
		do
			break
		done
	else 
		shift
	fi
}

function browser() {
	while [ "$1" ]
	do
		[ -f $1 ] && open $1
		shift
	done
}

function run() {
	#g clean jar testJar configureCluster test historicalYield sensorData enronEmails \
		#-PclusterVersion=${HV} 2>&1 | tee -a test-${HV}.out
	g clean jar testJar configureCluster $OPTS -PclusterVersion=${HV} --stacktrace 2>&1 | tee -a test-${HV}.out


	for i in "*/build/reports/tests/index.html"
	do
		if [ "`grep -i failed $i 2> /dev/null`" ]
		then
			echo "********** Found failing tests.  Exiting."
			browser $i
			FAILED=true
		fi

		if [ $FAILED ]
		then
			exit
		fi
	done
}

if [ "$HV" == "all" ]
then
	shift
	for HV in ${VERSIONS[@]}
	do
		> test-${HV}.out
	done
	for HV in ${VERSIONS[@]}
	do
		run $*
	done
else
	choose
	run $*
fi
