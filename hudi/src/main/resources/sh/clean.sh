#!/bin/bash
export num=`yarn app -list | grep hudi_ | wc -l`
if [[ ${num} == 0* ]] ; then
  echo 'no hudi job running, clean hdfs'
else
  echo 'find running hudi job, return' && exit 0
fi
hdfs dfs -rm -r /user/hive/.flink/*
hdfs dfs -rm -r /user/hive/.Trash/Current/user/hive/.flink/*