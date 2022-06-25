"""Create producer of real time trip updates from BART real time GTFS API"""

import time
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
from confluent_kafka import Producer


class BARTRealTime(object):

    def __init__(self):
        self.api_url = r'http://api.bart.gov/gtfsrt/tripupdate.aspx'
        self.kafka_topic = 'BART_Trips'
        self.kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def produce_trip_updates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self.api_url)
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                update_json = MessageToJson(entity.trip_update)
                self.kafka_producer.produce(self.kafka_topic, update_json.encode('utf-8'))

        self.kafka_producer.flush()

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(30)
