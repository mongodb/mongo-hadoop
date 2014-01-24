#! /bin/sh

for version in "0.23" "1.0" "1.1" "cdh4" "2.2"
do
	./gradlew -q -Phadoop_version=${version} clean $*
done
