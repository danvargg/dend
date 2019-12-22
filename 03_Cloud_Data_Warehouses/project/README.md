## Cloud Data Warehouse

### Introduction
A music streaming startup, `Sparkify`, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in `S3`, in a directory of `JSON` logs on user activity on the app, as well as a directory with `JSON` metadata on the songs in their app.

As their data engineer, you are tasked with building an `ETL` pipeline that extracts their data from `S3`, stages them in `Redshift`, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and `ETL` pipeline by running queries given to you by the analytics team from `Sparkify` and compare your results with their expected results.

### Project Description
Build an ETL pipeline for a database hosted on `Redshift`. Load data from `S3` to staging tables on `Redshift` and execute `SQL` statements that create the analytics tables from the staging tables.

### Data
- `s3://udacity-dend/log_data` contains songplay events of the users
- `s3://udacity-dend/song_data` contains list of songs details

### Database Schema
Dimension Tables:
- users
    - columns: `user_id`, `first_name`, `last_name`, `gender`, `level`
- songs
    - columns: `song_id`, `title`, `artist_id`, `year`, `duration`
- artists
    - columns: `artist_id`, `name`, `location`, `lattitude`, `longitude`
- time
    - columns: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

Fact Table:
- songplays
    - columns: `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Run the project

- Update the `dwh.cfg` file with your `Amazon Redshift` cluster credentials and `IAM role`
- Run `create_tables.py` to create the database and all the required tables
- Run `etl.py` to start the pipeline that will read the data from files and populate the tables
