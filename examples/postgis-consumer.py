# encoding: utf8
'''
Consume features from a kafka topic and write them to the db.

The expected DDL for the table is

CREATE TABLE public.sdkarma_live (
    id text,
    datetime text,
    geom geometry
);

'''

import psycopg2
import os.path
import logging
import sys
import os

# hack to load ukis_kafka from to parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))


from ukis_kafka.consumer import PostgresqlConsumer
from ukis_kafka.messagehandler.pg import PostgisInsertMessageHandler


# verbose logging
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter( logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root.addHandler(ch)


conn = psycopg2.connect(host='127.0.0.1', dbname='test_kafka', user='test', password='test')
cur = conn.cursor()
cur.execute('truncate sdkarma_live;')
conn.commit()

consumer = PostgresqlConsumer(conn,
        bootstrap_servers='localhost:9092',
        session_timeout_ms=10000,
        enable_auto_commit=False,
        client_id=os.path.basename(__file__),
        group_id=os.path.basename(__file__)
    )

consumer.register_topic_handler('sdkarma-live-wf', PostgisInsertMessageHandler('public', 'sdkarma_live'))

consumer.consume()
