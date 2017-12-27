

package etc.dataanalysis

import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.SparkContext;


object DataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Data Analysis Test");
    val sparkContext = new SparkContext(conf);
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext);
    val hive_tables1=hiveContext.sql("SET hive.auto.convert.join=false")
    val hive_tables2=hiveContext.sql("use project")
        
    val hive_tables3=hiveContext.sql("CREATE TABLE IF NOT EXISTS top_10_stations ( station_id STRING, total_distinct_songs_played INT, distinct_user_count INT ) PARTITIONED BY (batchid INT)  ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    val hive_tables4=hiveContext.sql("INSERT OVERWRITE TABLE top_10_stations PARTITION(batchid="+args(0)+") SELECT  station_id, COUNT(DISTINCT song_id) AS total_distinct_songs_played, COUNT(DISTINCT user_id) AS distinct_user_count FROM enriched_data WHERE status='pass' AND batchid="+args(0)+" AND like=1 GROUP BY station_id ORDER BY total_distinct_songs_played DESC LIMIT 10")
    val hive_tables5=hiveContext.sql("select * from top_10_stations")
    
    //hive_tables5.foreach(println)
    
    val hive_tables6=hiveContext.sql("CREATE TABLE IF NOT EXISTS users_behaviour ( user_type STRING, duration INT ) PARTITIONED BY (batchid INT) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    val hive_tables7=hiveContext.sql("INSERT OVERWRITE TABLE users_behaviour PARTITION(batchid="+args(0)+")  SELECT  CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type, SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data ed LEFT OUTER JOIN subscribed_users_lookup su ON ed.user_id=su.user_id WHERE ed.status='pass' AND ed.batchid="+args(0)+" GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END")
    val hive_tables8=hiveContext.sql("select * from users_behaviour")
    
    //hive_tables8.foreach(println)
    
    val hive_tables9=hiveContext.sql("CREATE TABLE IF NOT EXISTS connected_artists ( artist_id STRING, user_count INT ) PARTITIONED BY (batchid INT) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    val hive_tables10=hiveContext.sql("INSERT OVERWRITE TABLE connected_artists PARTITION(batchid="+args(0)+") SELECT ua.artist_id, COUNT(DISTINCT ua.user_id) AS user_count FROM ( SELECT user_id, artist_id FROM users_artists LATERAL VIEW explode(artists_array) artists AS artist_id ) ua INNER JOIN ( SELECT artist_id, song_id, user_id FROM enriched_data WHERE status='pass' AND batchid="+args(0)+" ) ed ON ua.artist_id=ed.artist_id AND ua.user_id=ed.user_id GROUP BY ua.artist_id ORDER BY user_count DESC LIMIT 10")
    val hive_tables11=hiveContext.sql("select * from connected_artists")
    
    //hive_tables11.foreach(println)
    
    val hive_tables12=hiveContext.sql("CREATE TABLE IF NOT EXISTS top_10_royalty_songs ( song_id STRING, duration INT ) PARTITIONED BY (batchid INT) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    val hive_tables13=hiveContext.sql("INSERT OVERWRITE TABLE top_10_royalty_songs PARTITION(batchid="+args(0)+") SELECT song_id, SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data WHERE status='pass' AND batchid="+args(0)+" AND (like=1 OR song_end_type=0) GROUP BY song_id ORDER BY duration DESC LIMIT 10")
    val hive_tables14=hiveContext.sql("select * from top_10_royalty_songs")
    
    //hive_tables14.foreach(println)
    
    val hive_tables15=hiveContext.sql("CREATE TABLE IF NOT EXISTS top_10_unsubscribed_users ( user_id STRING, duration INT ) PARTITIONED BY (batchid INT) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    val hive_tables16=hiveContext.sql("INSERT OVERWRITE TABLE top_10_unsubscribed_users PARTITION(batchid="+args(0)+") SELECT  ed.user_id, SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0))-CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration FROM enriched_data ed LEFT OUTER JOIN subscribed_users_lookup su ON ed.user_id=su.user_id WHERE ed.status='pass' AND ed.batchid="+args(0)+" AND (su.user_id IS NULL OR (CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0)))) GROUP BY ed.user_id ORDER BY duration DESC LIMIT 10")
    val hive_tables17=hiveContext.sql("select * from top_10_unsubscribed_users")
    
    //hive_tables17.foreach(println)
        
  }
}