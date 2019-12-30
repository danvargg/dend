## Data Lake

### Introduction
A music streaming startup, `Sparkify`, has grown their user base and song database even more and want to move their data warehouse to a `data lake`. Their data resides in `S3`, in a directory of JSON logs on `user activity` on the app, as well as a directory with JSON metadata on the `songs` in their app.

As their `data engineer`, you are tasked with building an `ETL pipeline` that extracts their data from `S3`, processes them using `Spark`, and loads the data back into `S3` as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Project Description
In this project, you'll apply what you've learned on `Spark` and `data lakes` to build an `ETL pipeline` for a data lake hosted on `S3`. To complete the project, you will need to load data from `S3`, process the data into analytics tables using `Spark`, and load them back into `S3`. You'll deploy this `Spark` process on a cluster using `AWS`.

### Data
- `s3://udacity-dend/log_data` contains songplay events of the users
- `s3://udacity-dend/song_data` contains list of songs details

### Database Schema
Dimension Tables:
- users: `user_id`, `first_name`, `last_name`, `gender`, `level`
- songs: `song_id`, `title`, `artist_id`, `year`, `duration`
- artists: `artist_id`, `name`, `location`, `lattitude`, `longitude`
- time: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

Fact Table:
- songplays: `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Project Structre
- `dl.cfg` contains AWS Credentials
- `etl.py` reads data from `S3`, processes that data using `Spark` and writes them back to `S3`
- `README.md` provides discussion on the process and decisions made

### ETL pipeline
- Load `AWS` credentials
- Read data from `S3`: `s3://udacity-dend/song_data` and `s3://udacity-dend/log_data`
- Process data using `Spark`: Transforms the data to create five different tables listed under `Dimension Tables` and `Fact Table`.
- Load data back to `S3`: Writes the data to partitioned `parquet` files in table directories on `S3`.
