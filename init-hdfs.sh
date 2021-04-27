#!/bin/bash

# script to init HDFS directories for MR jobs & Hive
echo 'creating the HDFS directories required to execute MapReduce jobs...'
$HADOOP_HOME/bin/hdfs dfs -mkdir /user
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/marco
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/marco/input
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/marco/output

echo 'creating HDFS directories required to Hive...'
$HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/hive
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -chmod /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod /user/hive/warehouse

