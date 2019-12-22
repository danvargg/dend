# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 2019
@author: danvargg
"""
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS public.events_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

# CREATE TABLES
staging_events_table_create = (
    """
    CREATE TABLE public.events_stage(
        artist_id VARCHAR ENCODE ZSTD,
        auth VARCHAR ENCODE ZSTD,
        first_name VARCHAR ENCODE ZSTD,
        gender VARCHAR ENCODE ZSTD,
        item_in_session VARCHAR ENCODE ZSTD,
        last_name VARCHAR ENCODE ZSTD,
        length FLOAT8 ENCODE ZSTD,
        level VARCHAR ENCODE ZSTD,
        location VARCHAR ENCODE ZSTD,
        method VARCHAR ENCODE ZSTD,
        page VARCHAR ENCODE ZSTD,
        registration VARCHAR ENCODE ZSTD,
        session_id VARCHAR ENCODE ZSTD,
        song_title VARCHAR ENCODE ZSTD,
        status VARCHAR ENCODE ZSTD,
        ts VARCHAR ENCODE ZSTD,
        user_agent VARCHAR ENCODE ZSTD,
        user_id VARCHAR ENCODE ZSTD);
    """)

staging_songs_table_create = (
    """
    """)

songplay_table_create = (
    """
    """)

user_table_create = (
    """
    """)

song_table_create = (
    """
    """)

artist_table_create = (
    """
    """)

time_table_create = (
    """
    """)

# STAGING TABLES
staging_events_copy = (
    """
    """).format()

staging_songs_copy = (
    """
    """).format()

# FINAL TABLES
songplay_table_insert = (
    """
    """)

user_table_insert = (
    """
    """)

song_table_insert = (
    """
    """)

artist_table_insert = (
    """
    """)

time_table_insert = (
    """
    """)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create, songplay_table_create,
    user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
    user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
    time_table_insert]
