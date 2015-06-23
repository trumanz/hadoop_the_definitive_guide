# hadoop_the_definitive_guide
## yarnExample 
the baisc program use YARN API
root@ag1:/local# su - hdfs -c "hdfs dfs -mkdir /user/root"
root@ag1:/local# su - hdfs -c "hdfs dfs -chown root /user/root"
root@ag1:/local#  hadoop jar yarnExample-0.0.1-SNAPSHOT.jar  trumanz.yarnExample.Client

