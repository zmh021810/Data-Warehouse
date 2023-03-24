## Project Readme Requests:
Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
State and justify your database schema design and ETL pipeline.



# Project: Data Warehouse



## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I need to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. I need to test my database and ETL pipeline by running queries given to you by the analytics team from Sparkify.



## Project Description and Analytical Goals
In this project, I need to build an ETL pipeline for a database hosted on Redshift. To complete the project, I need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.



## Project Datasets
These are the data path:
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Both of them are in jason format.



## Schema for Data Analysis
Using the song and event datasets, I create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplay - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
song - songs in music database
song_id, title, artist_id, year, duration
artist - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday




## ETL pipelines
First create some setups in AWS, this is the steps:
Create an IAM Role--Create Security Group--Launch a Redshift Cluster--Create an IAM User
The snapshot that cluster I created is in the image file

Then edit the dwh file and put the information which I created during the previous steps into this file

Then edit the sql_queries python file, and put the code for the queries, I add one more condition "event.page='NextSong'" since I remmeber during the first project we need to first filter as this.

Finally run the project, open a terminal and type "python create_tables.py", then type "python etl.py". Both of them passed, you can check the snapshot of the running in the image file