# encoding: utf8

from . import commons
from ..consumer import PostgresqlConsumer
from ..messagehandler.pg import PostgisInsertMessageHandler

import click
import psycopg2

def print_example_config(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    cfg = commons.Configuration({
        'logging': {
            'level': 'info',
            'file': '/tmp/example.log'
        },
        'postgresql': {
            'dsn': 'dbname=test user=tester password=secret'
        },
        'kafka': {
            'client_id': 'my-client-id',
            'group_id': 'my-group-id',
            'kafka_server': 'localhost:9092'
        },
        'topics': {
            'topic_a': [{
                'handler': 'postgisinsert',
                'table_name': 'mytable',
                'schema_name': 'public'
            }],
            'topic_b': [{
                'handler': 'postgisinsert',
                'table_name': 'mytable',
                'schema_name': 'public',
                'metafield_map': { # stores meta-field in db columns
                    'filename': 'id'
                },
                'property_map': { # no auto-mapping. manual field to db-column correlation
                    'area_km2': 'area_km2',
                    'datetime': 'datetime'
                }
            }]
        }
    })
    click.echo(cfg.yaml_dumps())
    ctx.exit()


def read_configuration(cfg_file):
    # base configuration
    config = commons.Configuration({
        'logging': {
            'level': 'info'
        }
    })
    config.yaml_read(cfg_file)
    return config


@click.command()
@click.option('--version', is_flag=True, callback=commons.print_version,
              expose_value=False, is_eager=True,
              help='Print version and exit.')
@click.option('--example_configuration', is_flag=True, callback=print_example_config,
              expose_value=False, is_eager=True,
              help='Print an example configuration file and exit.')
@click.argument('cfg_file', type=click.File(mode='r'))
def main(cfg_file):
    '''
    Consume vetor data from Kafka and write it to a PostGIS database.

    Attempts to autocast incomming ettributes to the types of the database
    columns. Timestamps are analyzed and brought into ISO-format when possible.

    Configuration is handled by a YAML configuration file. See the --example_configuration
    switch.
    '''

    # read the configuration and init
    config = read_configuration(cfg_file)
    commons.init_logging(config.get(('logging', 'level'), default='info'),
                logfile=config.get(('logging', 'logfile'), required=False))

    # establish a db connection
    conn = psycopg2.connect(config.get(('postgresql', 'dsn')))
    cur = conn.cursor()
    cur.execute('set application_name = %s', (click.get_current_context().info_name or '',))
    conn.commit()

    # connect to kafka
    consumer = PostgresqlConsumer(conn,
            bootstrap_servers=config.get(('kafka', 'kafka_server'), default='localhost:9092'),
            session_timeout_ms=10000,
            # commits are controlled by this tool to sync postgresql and kafka commits
            enable_auto_commit=False,
            client_id=config.get(('kafka', 'client_id')),
            group_id=config.get(('kafka', 'group_id'))
        )

    for topic_name in config.get(('topics',)).keys():
        for i in range(len(config.get(('topics', topic_name)))):
            handler_name = config.get(('topics', topic_name, i, 'handler'), default='postgisinsert')
            handler = None
            if handler_name == 'postgisinsert':
                handler = PostgisInsertMessageHandler(
                    cur,
                    config.get(('topics', topic_name, i, 'schema_name'), required=False),
                    config.get(('topics', topic_name, i, 'table_name'))
                )

                property_map = config.get(['topics', topic_name, i, 'property_map'], required=False)
                if property_map is not None:
                    handler.set_property_mapping(property_map)

                metafield_map = config.get(['topics', topic_name, i, 'metafield_map'], required=False)
                if metafield_map is not None:
                    handler.set_metafield_mapping(metafield_map)
            else:
                raise ValueError('unknown handler {0}'.format(handler_name))
            if handler:
                consumer.register_topic_handler(topic_name, handler)

    click.echo('Starting to consume')
    consumer.consume()
