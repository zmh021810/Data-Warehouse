import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# get the data, path and iamrole from the cfg file
iam_role= config.get("IAM_ROLE","ARN")
log_data= config.get("S3","LOG_DATA")
log_path= config.get("S3", "LOG_JSONPATH")
song_data= config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts TIMESTAMP,
userAgent VARCHAR,
userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
num_songs INTEGER,
year INTEGER,
duration FLOAT
)
""")

songplay_table_create = ("""
CREATE TABLE songplay(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP SORTKEY DISTKEY,
user_id INTEGER,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users(
user_id INTEGER SORTKEY PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE song(
song_id VARCHAR SORTKEY PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INTEGER,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artist(
artist_id VARCHAR SORTKEY PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longtitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time(
start_time TIMESTAMP SORTKEY DISTKEY PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER
)
""")

# STAGING TABLES


staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(log_data, iam_role, log_path)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(song_data, iam_role)








# FINAL TABLES

songplay_table_insert = ("""
 INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
 SELECT DISTINCT(event.ts) AS start_time,
        event.userId AS user_id,
        event.level AS level,
        song.song_id AS song_id,
        song.artist_id AS artist_id,
        event.sessionId AS session_id,
        event.location AS location,
        event.userAgent AS user_agent
 FROM staging_events event
 JOIN staging_songs song on (event.artist=song.artist_name AND event.song=song.title)
 AND event.page='NextSong'
""")

user_table_insert = ("""
 INSERT INTO users(user_id, first_name, last_name, gender, level)
 SELECT DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
 FROM staging_events
 WHERE page='NextSong' AND user_id is NOT NULL
""")

song_table_insert = ("""
 INSERT INTO song(song_id, title, artist_id, year, duration)
 SELECT DISTINCT(song_id) AS song_id, title, artist_id, year, duration
 FROM staging_songs
 WHERE song_id is NOT NULL
""")

artist_table_insert = ("""
 INSERT INTO artist(artist_id, name, location, latitude, longtitude)
 SELECT DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
 FROM staging_songs
 WHERE artist_id is NOT NULL  
""")

time_table_insert = ("""
 INSERT INTO time(start_time, hour, day, week, month, year, weekday)
 SELECT DISTINCT(start_time) AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
 FROM songplay
""")

# QUERY LISTS


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]





drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
