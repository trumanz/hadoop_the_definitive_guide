
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
