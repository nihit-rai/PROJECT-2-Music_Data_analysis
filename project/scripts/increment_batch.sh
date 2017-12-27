#!/bin/bash
batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid
echo "Incrementing batchid..." >> $LOGFILE
inc=`expr $batchid + 1`
echo -n $inc > /home/acadgild/project/logs/current-batch.txt