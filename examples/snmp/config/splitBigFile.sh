#/bin.bash/
for file in $(ls ./network_snmp_*)
do
    #echo $file
    fileSize=$(du -m $file)
    #echo $dufile
    for word in $fileSize
    do
        size=$word
        break
    done
   # echo $size
    if (( $size > "37" ))
    then
        echo "The size of "$file" is "$size" M."
        $(mv $file ./bigfile)
       # $(split -l 100000 $file ${file%.*})
    fi 
done

