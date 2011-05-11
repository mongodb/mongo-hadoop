

cd `dirname $0`

if ! [ -f IpToCountry.csv ] ; then
 wget software77.net/geo-ip/?DL=1 -O IpToCountry.csv.gz && gunzip IpToCountry.csv.gz
fi
 

hadoop fs -copyFromLocal  IpToCountry.csv IpToCountry.csv
