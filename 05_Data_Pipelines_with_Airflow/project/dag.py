# -*- coding: utf-8 -*-
"""
Created on Wed Jan 08 2020
@author: danvargg
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import sql_queries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 11),
}

dag = DAG('airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    s3_bucket="udacity-dend-airflow-test",
    s3_key="log_data",
    table="staging_events",
    create_stmt=sql_queries.create_table_staging_events
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    s3_bucket="udacity-dend-airflow-test",
    s3_key="song_data",
    table="staging_songs",
    create_stmt=sql_queries.create_table_staging_songs
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    create_stmt=sql_queries.create_table_songplays,
    sql_query=sql_queries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    create_stmt=sql_queries.create_table_users,
    sql_query=sql_queries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    create_stmt=sql_queries.create_table_songs,
    sql_query=sql_queries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    create_stmt=sql_queries.create_table_artist,
    sql_query=sql_queries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    create_stmt=sql_queries.create_table_time,
    sql_query=sql_queries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG operations sequence and dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

[load_song_dimension_table, load_user_dimension_table,
    load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
