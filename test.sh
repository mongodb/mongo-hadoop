#! /bin/sh

alias g="./gradlew --daemon"

OPTS=test

while [ "$1" ]
do
    case $1 in
        "examples")
            OPTS="historicalYield sensorData enronEmails"
            ;;
        "all")
            HV="all"
            ;;
    esac
	shift
done

echo Running \"$OPTS\"

function browser() {
	while [ "$1" ]
	do
		[ -f $1 ] && open $1
		shift
	done
}

function run() {
	g clean jar testJar $OPTS --stacktrace 2>&1 | tee -a build/test.out


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

run
