# PROJECT-2-Music_Data_analysis

A leading music-catering company is planning to analyse large amount of data received from
varieties of sources, namely mobile app and website to track the behaviour of users, classify users,
calculate royalties associated with the song and make appropriate business strategies. The file server
receives data files periodically after every 3 hours.

Data Analysis (SHOULD BE IMPLEMETED IN SPARK)
It is not only the data which is important, rather it is the insight it can be used to generate
important. Once we have made the data ready for analysis, we have to perform below analysis on a
daily basis.

1.	Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.
2.	Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'. An unsubscribed user is the one whose record is either not present in Subscribed_users lookup table or has subscription_end_date earlier than the timestamp of the song played by him.
3.	Determine top 10 connected artists. Connected artists are those whose songs are most listened by the unique users who follow them.
4.	 Determine top 10 songs who have generated the maximum revenue. Royalty applies to a song only if it was liked or was completed successfully or both.
5.	 Determine top 10 unsubscribed users who listened to the songs for the longest duration.

Challenges and Optimisations:
1.	LookUp tables are in NoSQL databases. Integrate them with the actual data flow.
2.	Try to make joins as less expensive as possible.
3.	Data Cleaning, Validation, Enrichment, Analysis and Post Analysis have to be automated. Try using schedulers.
4.	Appropriate logs have to maintained to track the behaviour and overcome failures in the pipeline.
Flow of operations
