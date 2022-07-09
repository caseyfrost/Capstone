"""Show status for each train: on-time, delayed + delay amount, early + early amount"""
import csv
import json

import arrow
import os
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from collections import defaultdict

bart_gtfs_dir = r'/Users/caseyfrost/Desktop/Springboard/GTFS_Capstone_Project/json_files/bart/routeinfo.nosync'
static_trips = os.path.join(bart_gtfs_dir, sorted(os.listdir(bart_gtfs_dir))[1], 'trips.txt')
static_stops = os.path.join(bart_gtfs_dir, sorted(os.listdir(bart_gtfs_dir))[1], 'stops.txt')


class BARTconsumer(object):
    def __init__(self):
        self.kafka_consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'bart_consumer',
            'default.topic.config': {
                'auto.offset.reset': 'latest'
            }
        })
        self.kafka_topic = 'BART_Trips'
        self.arrival_times = defaultdict(lambda: defaultdict(lambda: -1))
        self.stations = {}
        with open(static_stops) as stations:
            reader = csv.DictReader(stations)
            for row in reader:
                self.stations[row['stop_id']] = row['stop_name']
        self.trips = {}
        with open(static_trips) as trips:
            reader = csv.DictReader(trips)
            for row in reader:
                self.trips[row['trip_id']] = row['trip_headsign']

    def process_message(self, message):
        trip_update = json.loads(message)

        trip_header = trip_update.get('trip')
        if not trip_header or trip_header['scheduleRelationship'] == 'CANCELED':
            return

        trip_id = trip_header['tripId']
        stop_time_updates = trip_update.get('stopTimeUpdate')
        if not stop_time_updates or trip_id not in self.trips:
            return

        for update in stop_time_updates:
            delay = update['arrival']['delay']

            if 'arrival' not in update or 'stopId' not in update:
                continue

            stop_id = update['stopId']
            m, s = divmod(delay, 60)
            new_arrival_ts = int(update['arrival']['time'])

            next_arrival_ts = self.arrival_times[self.trips[trip_id]][stop_id]

            now = arrow.now(tz='US/Pacific')

            if new_arrival_ts >= now.timestamp() and (next_arrival_ts == -1 or new_arrival_ts < next_arrival_ts):
                self.arrival_times[self.trips[trip_id]][stop_id] = new_arrival_ts

                # convert time delta to minutes
                time_delta = arrow.get(new_arrival_ts) - now
                minutes = divmod(divmod(time_delta.seconds, 3600)[1], 60)[0]
                print(f'Next {self.trips[trip_id]} bound train will arrive at {self.stations[stop_id]} station in '
                      f'{minutes} minute(s). There is a {m} minute and {s} second delay.')

    def run(self):
        self.kafka_consumer.subscribe([self.kafka_topic])

        while True:
            msg = self.kafka_consumer.poll(1.0)

            if msg is None or not msg.value():
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                raise ValueError(f'Kafka consumer exception: {msg.error()}')

            msg = msg.value()
            self.process_message(msg.decode('utf-8'))


test = BARTconsumer()
test.run()
