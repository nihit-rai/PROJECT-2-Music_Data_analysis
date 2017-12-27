#!/bin/bash

batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Running hive script for data analysis..." >> $LOGFILE

spark-submit --class etc.dataanalysis.DataAnalysis --master local MusicDataAnalysis.jar $batchid