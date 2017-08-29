# encoding: utf8

from . import commons
from ..messagehandler.geojson import GeoJsonFileHandler
from ..consumer import BaseConsumer

import click
import sys

@click.command()
@click.option('--version', is_flag=True, callback=commons.print_version,
              expose_value=False, is_eager=True,
              help='Print version and exit.')
@click.option('--kafka_server', '-k', required=True, default='localhost:9092',
            help='Hostname and port of the kafka server to connect to.')
@click.option('--topic', '-t', required=True,
            help='Topic under which the vectors are published.')
@click.option('--client_id', '-c', required=True,
            help='Client_id for this consumer.')
@click.option('--group_id', '-g', required=True,
            help='Group_id for this consumer.')
@click.option('--filename', '-f', default="-",
            help='File to dump the received features to. Default is stdout ("-")')
@click.option('--loglevel', '-l', type=click.Choice(commons.loglevel_names()), default='error',
            help='Loglevel. logs will be written to stdout.')
@click.option('--output_format', '-F', default='geojson',
            type=click.Choice(['geojson']),
            help='Outputformat')
def main(kafka_server, topic, client_id, group_id, filename, loglevel, output_format):
    '''Consume features from a topic and dump them to file.'''
    commons.init_logging(loglevel)

    output = sys.stdout
    if filename != '-':
        output = open(filename, 'wb')

    consumer = BaseConsumer(
            bootstrap_servers=kafka_server,
            session_timeout_ms=10000,
            enable_auto_commit=False,
            client_id=client_id,
            group_id=group_id
    )

    handler = None
    if output_format == 'geojson':
        handler = GeoJsonFileHandler(output)
    else:
        raise Exception('Unsupported outputformat {0}'.format(output_format))

    consumer.register_topic_handler(topic, handler)
    consumer.consume()
