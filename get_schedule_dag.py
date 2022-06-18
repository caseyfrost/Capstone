"""Get the standard (not realtime) GTFS files from BART

The files are downloadable at the link below, and are packaged as a .zip"""

import requests
import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# getting the bart gtfs .zip
bart_gtfs_url = r"https://www.bart.gov/dev/schedules/google_transit.zip"  # BART gtfs url
bart_gtfs_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/routeinfo.nosync'


def get_schedule(url, out_file):
    response = requests.get(url)
    with open(out_file, 'wb') as output_file:
        output_file.write(response.content)


def get_todays_date():
    date = datetime.today().strftime('_%Y%m%d')
    return date


def create_filename(url, out_dir):
    date = datetime.today().strftime('_%Y%m%d')  # today's date
    filename = os.path.join(out_dir, url.split('/')[-1][:-4] + date + '.zip')  # out filepath
    return filename


def main(url, out_dir):
    bart_gtfs_filename = create_filename(url, out_dir)  # create output file path
    get_schedule(url, bart_gtfs_filename)  # download gtfs zip to file path
    date = get_todays_date()
    out_dir = os.path.join(bart_gtfs_dir, date)  # path to dir to unzip zip file
    shutil.unpack_archive(bart_gtfs_filename, out_dir)  # unzip the downloaded zip


default_args = {
    'owner': 'cfrost',
    'start_date': '2022-05-30',
    'end_date': '2023-05-30'
}

dag = DAG(
    dag_id='bart_gtfs_schedule',
    default_args=default_args,
    description='Fetches BART trip GTFS schedule once per day',
    schedule_interval='@daily',
    catchup=False
)

t0 = PythonOperator(
    task_id='bart_gtfs',
    dag=dag,
    python_callable=main,
    op_kwargs={'url': bart_gtfs_url, 'out_dir': bart_gtfs_dir}
)

t0
