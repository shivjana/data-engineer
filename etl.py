import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """This modelule reads the json song file into a pandas dataframe for songs and inserts appropriate columns and records to the songs and srtists table based on the design of the 2 tables"""
    
    # open song file
    df = pd.read_json(filepath,typ='series')
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)
  
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This module reads the json log file into a pandas dataframe, breaksdown timestamp to different units of time and inserts appropriate columns and records to users and time table based on the design of the 2 tables"""
    
    # open log file
    df = pd.read_json(filepath,lines=True) 

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = (t.dt.time,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df=pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    
    #Insert User Records
    for j, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    songplay_df = df[['artist','song','length','userId','level','itemInSession','location','userAgent','ts']].copy()
    songplay_df['ts'] = pd.to_datetime(songplay_df['ts'],unit='ms').dt.time
    cur.execute("rollback")
    for index, row in songplay_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,row.ts,row.userId,row.level,songid,artistid,row.itemInSession,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    



def process_data(cur, conn, filepath, func):
    """This module fetches all the files from the 2 directories and calls appropriate function received as argument in the function call"""
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Create connection to the database created using create_tables and calls the process_data module to process the files in the 2 directories namely data/song_data and data/log_data"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
