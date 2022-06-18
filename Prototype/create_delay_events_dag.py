"""Processes batches of trip update json files to create delay events
"""

import os
import json
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta


def df_to_db(database, table, dataframe):
    """Appends the given dataframe to the given database table"""
    conn_string = f'postgresql://postgres:ranch@localhost/{database}'
    db = create_engine(conn_string)
    conn = db.connect()

    dataframe.to_sql(table, con=conn, if_exists='append', index=False)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.commit()
    conn.close()


def yesterdays_date():
    yesterday = datetime.today() - timedelta(days=1)
    return yesterday.strftime('_%Y%m%d'), yesterday.strftime('%Y_%m_%d')


def get_date():
    date = datetime.today().strftime('%Y%m%d')
    return date


def create_trip_df(trip_update_dir):
    """Takes a folder of trip updates and returns a single filtered dataframe"""
    dataframes = []  # list to contain all dataframes for the day. One dataframe per trips.txt
    for filename in os.listdir(trip_update_dir):
        file = os.path.join(trip_update_dir, filename)
        with open(file, 'r') as cur_file:
            updates = json.load(cur_file)
            timestamp = updates['header']['timestamp']
            df = pd.json_normalize(updates['entity'], record_path=['trip_update', 'stop_time_update'], meta=['id'])
            df = df[df['arrival.delay'] != 0]
            df['timestamp'] = timestamp
            dataframes.append(df)
    concat_df = pd.concat(dataframes)  # concatenate all the dataframes into a single dataframe
    return concat_df


# path to the trip updates from the previous day
trips_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/trip_update.nosync'
# path to the GTFS routes and trips from the previous day
gtfs_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/routeinfo.nosync'


def main(trips_path, gtfs_path):
    dates = yesterdays_date()
    yesterdays_trips = os.path.join(trips_path, dates[1])  # creates path to directory of trip update files.

    # paths to normal gtfs files for extra data
    trips = os.path.join(gtfs_path, dates[0], 'trips.txt')
    routes = os.path.join(gtfs_path, dates[0], 'routes.txt')

    # desired columns from the GTFS files for their respective dataframes
    trip_cols = ['route_id', 'trip_id', 'trip_headsign']
    route_cols = ['route_id', 'route_short_name', 'route_long_name']

    trips_df = pd.read_csv(trips, usecols=trip_cols)  # create trips df which will provide the head-sign e.g. Daly City
    routes_df = pd.read_csv(routes, usecols=route_cols)  # routes df for route color and route name

    # add the acquired date and agency to the GTFS dataframes
    today = get_date()
    trips_df['agency'] = 'BART'
    trips_df['acquired_date'] = dates[1]
    routes_df['agency'] = 'BART'
    routes_df['acquired_date'] = dates[1]

    # push the dataframes to the database
    df_to_db('GTFS', 'Trips', trips_df)
    df_to_db('GTFS', 'Routes', routes_df)

    df = create_trip_df(yesterdays_trips)  # create dataframe from all the previous day's trip updates
    df.groupby(['id', 'stop_id'])['timestamp'].max()  # group by trip_id (id) and stop_id. Select the last delay
    dfx = df.groupby(['id', 'stop_id'])['timestamp'].transform(max) == df['timestamp']  # get indices of og df
    group_df = df[dfx].copy()  # create the new grouped df
    group_df['id'] = group_df['id'].astype(int)  # convert the id field from object to int for the next join
    group_df.rename(columns={'id': 'trip_id', 'arrival.delay': 'arrival_delay', 'arrival.time': 'arrival_time',
                             'arrival.uncertainty': 'arrival_uncertainty', 'departure.delay': 'departure_delay',
                             'departure.time': 'departure_time', 'departure.uncertainty': 'departure_uncertainty'}
                    , inplace=True)
    group_df['acquired_date'] = dates[1]  # add an acquired date field
    df_to_db('GTFS', 'Delays', group_df)  # push the df to the database


default_args = {
    'owner': 'cfrost',
    'start_date': '2022-06-13',
    'end_date': '2023-05-30'
}

dag = DAG(
    dag_id='bart_batch_process',
    default_args=default_args,
    description='Process the previous days trip updates and load into the database',
    schedule_interval='0 1 * * *',
    catchup=False
)

t0 = PythonOperator(
    task_id='bart_trips',
    dag=dag,
    python_callable=main,
    op_kwargs={'trips_path': trips_dir, 'gtfs_path': gtfs_dir}
)

t0
