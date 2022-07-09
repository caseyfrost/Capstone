"""Processes batches of trip update json files to create delay events
"""

import dask.dataframe as dd
import functools
import json
import os
import pandas as pd
import psycopg2
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from config import db_con_str
from multiprocessing import Pool

start_time = time.time()


def df_to_db(table, dataframe, conn_string):
    """Appends the given dataframe to the given database table"""
    db = create_engine(conn_string)
    conn = db.connect()

    dataframe.to_sql(table, uri=conn_string, if_exists='append', index=False)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.commit()
    conn.close()


def create_trip_update_csv(trip_update_json, json_dir, out_csv_dir):
    """Takes a trip update json, normalizes the columns into a dataframe, and exports to a csv"""
    file = os.path.join(json_dir, trip_update_json)
    with open(file, 'r') as cur_file:
        updates = json.load(cur_file)
        timestamp = updates['header']['timestamp']
        df = pd.json_normalize(updates['entity'], ['trip_update', 'stop_time_update'],
                               ['id', ['trip_update', 'trip', 'schedule_relationship']])
        df = df[df['trip_update.trip.schedule_relationship'] != 3]
        df = df.drop(['trip_update.trip.schedule_relationship'], axis=1)
        df = df[df['arrival.delay'] != 0]
        df['timestamp'] = timestamp
        out_file = os.path.join(out_csv_dir, f'{timestamp}.csv')
        df.to_csv(out_file, index=False)


# path to the trip updates from the previous day
trips_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/trip_update.nosync'
csv_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/processed_csv'
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y_%m_%d')
in_json = os.path.join(trips_dir, yesterday)
out_csv = os.path.join(csv_dir, yesterday)


def main(in_json_dir, out_csv_dir):
    if not os.path.isdir(out_csv_dir):
        os.mkdir(out_csv_dir)

    # p = Pool(8)
    # p.map(functools.partial(create_trip_update_csv, out_csv_dir=out_csv_dir, json_dir=in_json_dir),
    #       os.listdir(in_json_dir))
    for file_name in os.listdir(in_json_dir):
        create_trip_update_csv(file_name, in_json_dir, out_csv_dir)

    csv_files = f'{out_csv_dir}/*.csv'
    ddf = dd.read_csv(csv_files)

    group_df = ddf.groupby(['id', 'stop_id'])['timestamp'].agg({'timestamp': 'max'})
    group_df = group_df.reset_index()
    group_df = group_df.rename(columns={'timestamp': 'timestamp_max'})
    ddf = ddf.merge(group_df, how='left', on=['id', 'stop_id'])
    group_df = ddf[ddf['timestamp'] == ddf['timestamp_max']]
    group_df = group_df.drop(['timestamp_max'], axis=1)
    group_df['id'] = group_df['id'].astype(int)  # convert the id field from object to int for the next join
    group_df = group_df.rename(columns={'id': 'trip_id', 'arrival.delay': 'arrival_delay',
                                        'arrival.time': 'arrival_time', 'arrival.uncertainty': 'arrival_uncertainty',
                                        'departure.delay': 'departure_delay', 'departure.time': 'departure_time',
                                        'departure.uncertainty': 'departure_uncertainty'})
    group_df['acquired_date'] = yesterday  # add an acquired date field
    df_to_db('Delays', group_df, db_con_str)  # write the delay events to the database


if __name__ == '__main__':
    main(in_json, out_csv)

    print('time: ' + str(time.time() - start_time))

default_args = {
    'owner': 'cfrost',
    'start_date': '2022-06-13',
    'end_date': '2023-05-30'
}

dag = DAG(
    dag_id='bart_create_delay_events_optimized',
    default_args=default_args,
    description='Process the previous days trip updates and load into the database',
    schedule_interval='0 1 * * *',
    catchup=False
)

t0 = PythonOperator(
    task_id='create_delays',
    dag=dag,
    python_callable=main,
    op_kwargs={'in_json_dir': in_json, 'out_csv_dir': out_csv}
)

t0
