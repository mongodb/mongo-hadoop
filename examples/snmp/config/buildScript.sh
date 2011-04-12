#file1="instruction.js"
#file2="snmp.js"
#foo = ()
size_ver=0
      
if test -f network_snmp_*
then
    rm -r network_snmp_*
fi

if test -f instruction.js
then
    rm -r instruction.js
fi

if test -f snmp.js
then
    rm -r snmp.js
fi

#for file in $(ls ./*/*.gz);
for file in $(ls ./0402*/*.gz)
do
  echo "$file"
  insertHead=0
  gunzip < $file |  while read line;
  do
    #echo $line
    if [[ $line == "#V3"* ]]; then
      export IFS=","
      for word in $line; do
          if test $size_ver == "3"; then
             echo $word
             date=$word
             insertHead="db.snmp.insert({"
          fi
          let size_ver++
      done
      export IFS=" "
    elif [[ $line == "if"*  ]]; then
          echo $insertHead"\"date\":\"$date\",\"key\":\"if\",\"content\":\"$line\"});" >> network_snmp_$date.js
    elif [[ $line == "c1"* ]]; then
          echo $insertHead"\"date\":\"$date\",\"key\":\"c1\",\"content\":\"$line\"});" >> network_snmp_$date.js
    elif [[ $line == "c2"*  ]]; then
          echo $insertHead"\"date\":\"$date\",\"key\":\"c2\",\"content\":\"$line\"});" >> network_snmp_$date.js
    elif [[ $line == "sys"*  ]]; then
          echo $insertHead"\"date\":\"$date\",\"key\":\"sys\",\"content\":\"$line\"});" >> network_snmp_$date.js
    fi
  done
done    
exit 0
