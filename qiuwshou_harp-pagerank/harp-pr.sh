#!/bin/bash

cd /root/software/extra/harp2-project-master/harp2-app
ant 
cp /root/software/extra/harp2-project-master/harp2-app/build/harp2-app-hadoop-2.6.0.jar /root/software/extra/hadoop-2.6.4

hadoop fs -rm pr/input5k/pr_0
hadoop fs -rm pr/input5k/pr_1

hdfs dfs -mkdir -p pr/input5k
hdfs dfs -put ~/input/pr_* pr/input5k

hadoop jar /root/software/extra/harp2-project-master/harp2-app/build/harp2-app-hadoop-2.6.0.jar edu.iu.simplepagerank.HarpPageRank pr/input5k pr/output5k 5000 10

#hdfs dfs -get pr/output5k
hadoop fs -copyToLocal pr/output5k/part-m-00000 ~/






