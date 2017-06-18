#!/bin/bash

cd /root/software/extra/harp2-project-master/harp2-app
ant 
cp /root/software/extra/harp2-project-master/harp2-app/build/harp2-app-hadoop-2.6.0.jar /root/software/extra/hadoop-2.6.4

 
hadoop fs -rm harpkmeans/out/kmeans_out_0/_SUCCESS
hadoop fs -rm harpkmeans/out/kmeans_out_0/part-m-00000
hadoop fs -rm harpkmeans/out/kmeans_out_0/part-m-00001
hadoop fs -rm harpkmeans/centroids/centroids_0

hadoop jar /root/software/extra/harp2-project-master/harp2-app/build/harp2-app-hadoop-2.6.0.jar edu.iu.simplekmeans.KmeansMapCollective 100 12 3 2 20 harpkmeans /tmp/simplekmeansdata

#hdfs dfs -get harpkmeans/out

cd ~/
rm -f centroids_0
hadoop fs -copyToLocal harpkmeans/centroids/centroids_0 ~/

rm -f part-m-00000
rm -f part-m-00001

hadoop fs -copyToLocal harpkmeans/out/kmeans_out_0/part-m-00000 ~/
hadoop fs -copyToLocal harpkmeans/out/kmeans_out_0/part-m-00001 ~/

