# encoding: utf8

from ..wireformat.fiona import feature_to_wireformat
from ..wireformat import pack
from . import commons

from kafka import KafkaProducer
import click
import fiona

import logging

logger = logging.getLogger(__name__)

@click.command()
@click.option('--version', is_flag=True, callback=commons.print_version,
              expose_value=False, is_eager=True)
@click.option('--kafka_server', '-k', required=True, default='localhost:9092',
            help='Hostname and port of the kafka server to connect to.')
@click.option('--topic', '-t', required=True,
            help='Topic under which the vectors are published.')
@click.option('--loglevel', '-l', type=click.Choice(commons.loglevel_names()), default='info',
            help='Loglevel. logs will be written to stdout.')
@click.argument('filename', type=click.Path(exists=True))
def main(filename, kafka_server, topic, loglevel):
    '''
    Read vector geodata from a file and push it into Kafka.
    '''
    commons.init_logging(loglevel)

    producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            key_serializer=pack.pack,
            value_serializer=feature_to_wireformat,
            compression_type='gzip'
        )

    try:
        f_counter = 0
        with fiona.open(filename, 'r') as src:
            for f in src:
                producer.send(topic, f)
                f_counter += 1
                if f_counter % 100 == 0:
                    print("Send {0} features".format(f_counter))
            print("Send {0} features".format(f_counter))
    finally:
        producer.flush()
