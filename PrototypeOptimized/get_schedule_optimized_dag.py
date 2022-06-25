"""Get the standard (not realtime) GTFS files from BART

The files are downloadable at the link below, and are packaged as a .zip"""

import requests
import os
import pandas as pd
import shutil
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import db_con_str
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# getting the bart gtfs .zip
bart_gtfs_url = r"https://www.bart.gov/dev/schedules/google_transit.zip"  # BART gtfs url
bart_gtfs_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/routeinfo.nosync'

# today's date with underscore in front
todays_date = datetime.today().strftime('_%Y%m%d')


def df_to_db(table, dataframe, conn_string):
    """Appends the given dataframe to the given database table"""
    db = create_engine(conn_string)
    conn = db.connect()

    dataframe.to_sql(table, con=conn, if_exists='append', index=False)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.commit()
    conn.close()


def get_schedule(url, out_file):
    response = requests.get(url)
    with open(out_file, 'wb') as output_file:
        output_file.write(response.content)


def create_filename(url, out_dir, today):
    filename = os.path.join(out_dir, url.split('/')[-1][:-4] + today + '.zip')  # out filepath
    return filename


def static_gtfs_to_csv(url, out_dir, date):
    bart_gtfs_filename = create_filename(url, out_dir, date)  # create output file path
    get_schedule(url, bart_gtfs_filename)  # download gtfs zip to file path
    out_dir = os.path.join(bart_gtfs_dir, date)  # path to dir to unzip zip file
    shutil.unpack_archive(bart_gtfs_filename, out_dir)  # unzip the downloaded zip


def static_gtfs_to_db(gtfs_csv_dir, date):
    # paths to normal gtfs files for extra data
    trips = os.path.join(gtfs_csv_dir, date, 'trips.txt')
    routes = os.path.join(gtfs_csv_dir, date, 'routes.txt')

    # desired columns from the GTFS files for their respective dataframes
    trip_cols = ['route_id', 'trip_id', 'trip_headsign']
    route_cols = ['route_id', 'route_short_name', 'route_long_name']

    trips_df = pd.read_csv(trips, usecols=trip_cols)  # create trips df which will provide the head-sign e.g. Daly City
    routes_df = pd.read_csv(routes, usecols=route_cols)  # routes df for route color and route name

    # add the acquired date and agency to the GTFS dataframes
    today = datetime.today().strftime('%Y_%m_%d')
    trips_df['agency'] = 'BART'
    trips_df['acquired_date'] = today
    routes_df['agency'] = 'BART'
    routes_df['acquired_date'] = today

    # push the dataframes to the database
    df_to_db('Trips', trips_df, db_con_str)
    df_to_db('Routes', routes_df, db_con_str)


default_args = {
    'owner': 'cfrost',
    'start_date': '2022-05-30',
    'end_date': '2023-05-30',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id='bart_gtfs_schedule_optimized',
    default_args=default_args,
    description='Fetches BART trip GTFS schedule once per day',
    schedule_interval='@daily',
    catchup=False
)

t0 = PythonOperator(
    task_id='bart_gtfs_to_csv',
    dag=dag,
    python_callable=static_gtfs_to_csv,
    op_kwargs={'url': bart_gtfs_url, 'out_dir': bart_gtfs_dir, 'date': todays_date}
)

t1 = PythonOperator(
    task_id='gtfs_csv_to_db',
    dag=dag,
    python_callable=static_gtfs_to_db,
    op_kwargs={'gtfs_csv_dir': bart_gtfs_dir, 'date': todays_date}
)

t0 >> t1
