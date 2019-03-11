#!python
# -*- coding: utf-8 -*-
"""
"""

import os
import random
import sys
import time
import logging

# hack to load ukis_kafka from to parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))

from ukis_kafka.wireformat.fiona import feature_to_wireformat
from ukis_kafka.wireformat import pack

from kafka import KafkaProducer
import fiona

# verbose logging
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter( logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root.addHandler(ch)


producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=pack.pack,
        value_serializer=feature_to_wireformat,
        compression_type='gzip'
    )


filename = os.path.join(
        os.path.dirname(__file__),
        'data/sdkarma-flood-live/sdkama:sdkama_flood_live.shp'
)

with fiona.open(filename, 'r') as src:
    for f in src:
        print(("Sending {0}".format(f['properties']['datetime'])))
        producer.send('sdkarma-live-wf', f)

        time.sleep(random.randint(0, 1)) # delay

producer.flush()
