#!/bin/bash

#script to start dfs

echo 'formatting namenode...'
$HADOOP_HOME/bin/hdfs namenode -format

echo 'running start-dfs.sh'
$HADOOP_HOME/sbin/start-dfs.sh
