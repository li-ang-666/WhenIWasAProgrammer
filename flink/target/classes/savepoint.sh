#!/bin/bash

file="savepoint.log"
rm $file


yid_and_info=$(yarn application -list 2>&1 | grep 'HCM' | grep -v 'DORIS')
for elem in $yid_and_info
do
echo $elem >> $file
done


yid=$(cat $file | grep 'application_')


fid_and_info=$(flink list -yid $yid | grep 'DataSyncJob')
for elem in $fid_and_info
do
echo $elem >> $file
done


fid=$(cat $file | egrep '[0-9a-z]{20,}')


echo $yid
echo $fid


flink savepoint -yid $yid $fid
