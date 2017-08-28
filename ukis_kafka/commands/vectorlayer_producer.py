# encoding: utf8

from ..wireformat.fiona import feature_to_wireformat
from ..wireformat import pack
from . import commons

from kafka import KafkaProducer
import click
import fiona

import logging
import uuid

logger = logging.getLogger(__name__)

@click.command()
@click.option('--version', is_flag=True, callback=commons.print_version,
              expose_value=False, is_eager=True,
              help='Print version and exit.')
@click.option('--kafka_server', '-k', required=True, default='localhost:9092',
            help='Hostname and port of the kafka server to connect to.')
@click.option('--topic', '-t', required=True,
            help='Topic under which the vectors are published.')
@click.option('--loglevel', '-l', type=click.Choice(commons.loglevel_names()), default='info',
            help='Loglevel. logs will be written to stdout.')
@click.option('--meta_field', '-m', multiple=True,
            help='Add a field with metainformation to the data. This option can be used multiple times. Syntax: KEY=VALUE')
@click.argument('filename', type=click.Path(exists=True))
def main(filename, kafka_server, topic, loglevel, meta_field):
    '''
    Read vector geodata from a file and push it into Kafka.
    '''
    commons.init_logging(loglevel)

    # collect meta information to attach to the features
    meta = {
        # unique identifier for this invocation/batch
        'batch': uuid.uuid4().hex
    }
    for mf in meta_field:
        spos = mf.index('=')
        if spos < 1:
            raise RuntimeError("Meta field can not be parsed: {0}".format(mf))
        meta[mf[:spos]] = mf[spos+1:]

    def value_serializer(*a, **kw):
        '''custom serializer to attach meta infos'''
        kw['meta'] = meta
        return feature_to_wireformat(*a, **kw)

    producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            key_serializer=pack.pack,
            value_serializer=value_serializer,
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
            if f_counter % 100 != 0:
                print("Send {0} features".format(f_counter))
    finally:
        producer.flush()
