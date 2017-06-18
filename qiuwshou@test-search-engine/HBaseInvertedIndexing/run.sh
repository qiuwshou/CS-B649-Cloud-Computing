#!/bin/bash

cd /root/MoocHomeworks/HBaseInvertedIndexing/
ant
cp /root/MoocHomeworks/HBaseInvertedIndexing/dist/lib/cglHBaseMooc.jar /root/software/hadoop-1.1.2/lib/
cd /root/software/hadoop-1.1.2/
./bin/hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.SearchEngineTester search-keyword snapshot
./bin/hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.SearchEngineTester get-page-snapshot 00000113548 | grep snapshot
