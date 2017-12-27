#!/bin/bash

#This script is not working.
#Either change table to text or use STRING as type of partitioned column

batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Running sqoop job for data export..." >> $LOGFILE

sqoop export \
--connect jdbc:mysql://localhost/musicdata \
--username 'root' \
--table top_10_stations \
--export-dir hdfs://localhost:9000/user/hive/warehouse/musicdata.db/top_10_stations/batchid=$batchid \
--input-fields-terminated-by ',' \
-m 1

sqoop export \
--connect jdbc:mysql://localhost/musicdata \
--username 'root' \
--table users_behaviour \
--export-dir hdfs://localhost:9000/user/hive/warehouse/musicdata.db/users_behaviour/batchid=$batchid \
--input-fields-terminated-by ',' \
-m 1

sqoop export \
--connect jdbc:mysql://localhost/musicdata \
--username 'root' \
--table connected_artists \
--export-dir hdfs://localhost:9000/user/hive/warehouse/musicdata.db/connected_artists/batchid=$batchid \
--input-fields-terminated-by ',' \
-m 1

sqoop export \
--connect jdbc:mysql://localhost/musicdata \
--username 'root' \
--table top_10_royalty_songs \
--export-dir hdfs://localhost:9000/user/hive/warehouse/musicdata.db/top_10_royalty_songs/batchid=$batchid \
--input-fields-terminated-by ',' \
-m 1

sqoop export \
--connect jdbc:mysql://localhost/musicdata \
--username 'root' \
--table top_10_unsubscribed_users \
--export-dir hdfs://localhost:9000/user/hive/warehouse/musicdata.db/top_10_unsubscribed_users/batchid=$batchid \
--input-fields-terminated-by ',' \
-m 1
