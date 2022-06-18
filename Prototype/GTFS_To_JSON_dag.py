"""Creates json file of a GTFS trip update.

The program takes a path to a directory, creates an output filename using the current time, gets the trip update
from the API url, converts the FeedMessage to a json object, and writes the json object to the file name.
"""

import json
import os
import time
import urllib.request
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict


trip_update_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/trip_update.nosync'
bart_trip_updt_url = r'http://api.bart.gov/gtfsrt/tripupdate.aspx'


def crt_out_dir(parent_dir):
    date = time.strftime('%Y_%m_%d')
    path = os.path.join(parent_dir, date)
    if os.path.isdir(path):
        return path
    else:
        os.mkdir(path)
        return path


def crt_out_file(out_dir):
    """Creates output path for file with filename unique to every second.

    Args:
        out_dir: path to desired output directory
    Returns:
        the path to the output file"""

    date = time.strftime('%Y_%m_%d-%H_%M_%S_%p')
    filename = f'BART_{date}.json'
    out_file_path = os.path.join(out_dir, filename)
    return out_file_path


def crt_trip_update_json(url):
    """Converts a GTFS API FeedMessage into a JSON object.

    Args:
        url: API url for trip update, generally http://.../.../tripupdate.aspx
    Returns:
        A JSON object of the data returned by the API request."""
    
    gtfs_feed = gtfs_realtime_pb2.FeedMessage()
    gtfs_feed.ParseFromString(urllib.request.urlopen(url).read())
    gtfs_dict = protobuf_to_dict(gtfs_feed)
    return json.dumps(gtfs_dict, indent=4, sort_keys=True)


def write_json(path, js):
    with open(path, 'w') as outfile:
        outfile.write(js)


def main(parent_dir, api_url):
    out_dir = crt_out_dir(parent_dir)
    out_file = crt_out_file(out_dir)
    json_obj = crt_trip_update_json(api_url)
    write_json(out_file, json_obj)


default_args = {
    'owner': 'cfrost',
    'start_date': '2022-05-30',
    'end_date': '2023-05-30'
}

dag = DAG(
    dag_id='bart_trip_updates',
    default_args=default_args,
    description='Fetches BART trip updates from the real time API every minute',
    schedule_interval='* * * * *',
    catchup=False
)

t0 = PythonOperator(
    task_id='bart_trips',
    dag=dag,
    python_callable=main,
    op_kwargs={'parent_dir': trip_update_dir, 'api_url': bart_trip_updt_url}
)

t0
