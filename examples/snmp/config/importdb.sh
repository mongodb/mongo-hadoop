#bin/bash/
#mongo 127.0.0.1:30000/admin setup-shards.js
for file in $(ls network_snmp_*)
do
    echo "importing "$file" to mongodb"
    mongo localhost:30000 $file
done
