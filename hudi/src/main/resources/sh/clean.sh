#!/bin/bash
export num=`yarn app -list | grep hudi_ | wc -l`
if [[ ${num} == 0* ]] ; then
  echo 'no hudi job running, clean hdfs'
  hdfs dfs -rm -r /user/hive/.flink/*
  hdfs dfs -rm -r /user/hive/.Trash/Current/user/hive/.flink/*
else
  echo 'find running hudi job, exit'
fi