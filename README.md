
# OBJECTIVE

Sparkify is a music streaming app and as part of user-analysis, they analyze the data which are mainly composed of log files on songs and user activity. The primary objective of their analysis is to determine what songs users are mostly interested in and also determine an easier way to use the JSON log files which are residing in a directory. 

## DATABASE DESIGN

So as part of this, the database must be designed in such a way that that we can create a optimized query on both the songs and the users. Star schema will be used for the schema design with 5 tables namely songplay, which will be the fact table containing information about the user usage and hence a fact table, users, which will have information about the users using the app, songs, which will have information about the songs played in the app, artists, which will have information about the artists playing the songs played in the app, and time, which will have information about the timestamps of the records in songplay broken into time,day,month,week,weekday and year. The songplay table will be associated through the primary keys of the dimension tables, namely user_id from the users table, song_id from the song table, artist_id from the artists table, and start_time from time table.Since the primary objective of the analysis is to understand what the users are listening to, the songplay table will be helpful in gving us facts related to usage by combining the information referred from other dimension tables

## ETL PIPELINING

Another objective of the analysis is to make querying the data a lot easier. The JSON log files are located 2 directories, one containing user activity related files and other containing song related files. Pandas dataframe will be used to read the json files and push into a dataframe which is then ready row by row and then inserted into the appropriate tables in the database. For the time table, since we need to break down the timestamp into different time units, the timestamp read from the log file will be converted into a datetime object and then broken down using the dt attribute. The songs and artist table are populated by reading the log files from the directory containing song and artist data. The users and time table are populated by reading the timestamped log files in another directory. In order to fetch the song_id and artist_id from the song and artist table for populating in songplay table, song and artist table are joined together and matched against the duration, song title and artist name from the log files. The other attributes in songplay are inserted by reading the timestamped files.


