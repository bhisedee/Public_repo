from datetime import datetime, timedelta
import pendulum
import os
import pandas as pd
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

# define date to pickup the log data from S3 bucket, this is to be fed as parameter to stage operator. 
# when required it is to be used as execution_time parametes or do backfilles. 
# currently as we have data in S3 for 2018, Nov month. date is set for same date. later used in variable S3_LOG_KEY
DATE_RUN = datetime(2018, 11, 1, 0, 0, 0)

S3_BUCKET_NAME = 'bhise-dee'
REGION = 'us-east-1'
LOG_JSON_PATH = "s3://bhise-dee/log_json/log_json_path.json"
#S3 log Key, this allows to only load specific year and month data from the S3 location. 
S3_LOG_KEY = f'log-data/{DATE_RUN.year}/{DATE_RUN.month}'
#S3_LOG_KEY = 'log-data/2018/11'
S3_SONG_KEY = 'song-data'

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *' # scheduled to run hourly
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id = 'end_execution')
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id ='Stage_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        table = 'staging_events',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = S3_LOG_KEY,
        truncate = True,
        json_format = LOG_JSON_PATH
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        table = 'staging_songs',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = S3_SONG_KEY,
        truncate = True,
        json_format = "auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplays',
        sql = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        sql = SqlQueries.user_table_insert,
        append = True,
        table = 'users'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        sql = SqlQueries.song_table_insert,
        append = True,
        table = 'songs'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        sql = SqlQueries.artist_table_insert,
        append = True,
        table = 'artists'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        sql = SqlQueries.time_table_insert,
        append = True,
        table = 'times'
    )
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        table = 'songplays'
    )
    
    start_operator >> [ stage_events_to_redshift , stage_songs_to_redshift ] >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()