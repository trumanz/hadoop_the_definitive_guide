
1. Download source data 
ftp://ftp.ncdc.noaa.gov/pub/data/noaa/

2. Build package
  cd MaxTemperature && mvn package

4. Transfer source data to HDFS
   root@ag1:/local/ch02-mapreduce/data# su - hdfs -c "hdfs  dfs -chown 777  /tmp"
   root@ag1:/local/ch02-mapreduce/data# hdfs dfs -mkdir /tmp/maxtemp/
   root@ag1:/local/ch02-mapreduce/data# hdfs dfs -mkdir /tmp/maxtemp/input
   root@ag1:/local/ch02-mapreduce/data# hdfs dfs -put 5record   /tmp/maxtemp/input
   root@ag1:/local/ch02-mapreduce/data# hdfs dfs -chmod 777  /tmp/maxtemp

5. Run the mapreduce
   root@ag1:/local/ch02-mapreduce/MaxTemperature/target# su hdfs -c  "hadoop jar MaxTemperature-0.0.1-SNAPSHOT.jar   trumanz.MaxTemperature.MaxTemperature   /tmp/maxtemp/input   /tmp/maxtemp/output"

6. Run the Streaming mapreduce(python)
  root@ag1:/local/ch02-mapreduce/data# su hdfs -c  "hadoop jar /usr/hdp/2.2.4.2-2/hadoop-mapreduce/hadoop-streaming-2.6.0.2.2.4.2-2.jar -input  /tmp/maxtemp/input   -output  /tmp/maxtemp/output2   -mapper ../MaxTemperature/src/main/python/max_temperature_map.py  --reducer  ../MaxTemperature/src/main/python/max_temperature_reduce.py"

