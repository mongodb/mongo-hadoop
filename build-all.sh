#! /bin/sh

for version in "0.23" "1.0" "1.1" "2.2" "cdh4" "cdh5" "2.3" "2.4"
do
./gradlew -Phadoop_version=${version} clean $* # 2>&1 | tee gradle-${version}.out
done
