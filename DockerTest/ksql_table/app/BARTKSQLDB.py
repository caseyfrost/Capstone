"""BART KSQLDB class connects to api, creates a stream from the normalized topic, and lastly creates a table that
aggregates records in 2 minute hopping windows of 1 hour each to find the average delay by headsign"""

import time
from ksql import KSQLAPI


class BARTKSQLDBTable(object):

    def __init__(self):
        self.client = KSQLAPI('http://primary-ksqldb-server:8088', timeout=360)

    def create_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_STREAM_TESTING (id VARCHAR KEY, trip_id VARCHAR, delay BIGINT, stop_name VARCHAR, headsign VARCHAR) WITH (kafka_topic='BART_Trips_Normalized', value_format='json')")

    def create_avro_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_STREAM_AVRO WITH (VALUE_FORMAT='AVRO', KAFKA_TOPIC='bart_trips_avro') AS SELECT * FROM BART_STREAM_TESTING")

    def create_table(self):
        self.client.ksql("CREATE OR REPLACE TABLE bart_avg WITH (KAFKA_TOPIC='bart_delays') AS SELECT headsign, AVG(delay) FROM BART_STREAM_AVRO WINDOW SESSION (1 HOURS) GROUP BY headsign")

    def create_final_table(self):
        self.client.ksql("CREATE OR REPLACE TABLE bart_delays WITH (KAFKA_TOPIC='bart_delays') AS SELECT * FROM bart_avg")

    def run(self):
        self.create_stream()
        time.sleep(10)
        self.create_avro_stream()
        time.sleep(10)
        self.create_table()
        self.query()

    def query(self):
        q = self.client.query("SELECT * FROM bart_avg EMIT CHANGES")
        for r in q:
            print(r)


def main():
    time.sleep(300)
    test = BARTKSQLDBTable()
    test.run()


if __name__ == '__main__':
    main()
