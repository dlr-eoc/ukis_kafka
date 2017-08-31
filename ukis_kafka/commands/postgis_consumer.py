# encoding: utf8

from . import commons
from ..consumer import PostgresqlConsumer
from ..messagehandler.pg import PostgisInsertMessageHandler

import click
import psycopg2

def print_example_config(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    explanation = '''
Configuration file for {prog_name}

Logging section
===============

* The specified logfile will automaticaly be rotated.
* When no logfile is set, the log will be written to stdout.
* The available log levels are: {log_levels}.

Postgresql section
==================

* The 'dsn'-Parameter is mandatory and used to connect to the
  database server. The syntax is libpqs connection string format
  and documented under 
  https://www.postgresql.org/docs/9.6/static/libpq-connect.html

Kafka section
=============

* All parameters are mandatory.

Topics section
==============

This configuration section contains a mapping of topic names to subscribe to and 
handler chains where the received messages are passed through. This
means it is possible to specify more than one handler to process messages
multiple times.

Each handler configuration consists of the mandatory name of the handler in
the "handler" parameter and a handler specific number of settings.

postgisinsert Handler
---------------------

This handler inserts incomming messages into the database.
PostGIS geometries are supported. Transactions are supported, each message
is inserted with its own savepoint. So a rollback only needs to happen for the
last message when inserting fails. Messages which can not be inserted in the
database will be discarded.
Incomming values are cast to the type of the database columns when possible and there
are sanitation routines for types like timestamps to attempt to make them understandable for
the database server.

The 'table_name' and 'schema_name' settings specify the target table and are required.

'property_map' maps the properties of the features of the messages to database columns. The keys
are the names of the features properties, the values the names of the db columns. When this setting
is not set {prog_name} performs an automapping and correlates properties and columns by their names.

'metafield_map' maps the fields of the messages 'meta' attribute to database columns. The keys
are the names of the features meta fields, the values the names of the db columns.
When not set, no mapping will be performed.

'predefined_values' is a set of values defined in the configuration file which will be inserted
into the database columns. The keys are the names of the database columns, the values are the values.

The 'on_conflict' settings is optional and supports PostgreSQLs INSERT-conflict handling.
Possible values are 'do nothing' and 'do update'. This setting requires PostgreSQL 9.5. 
For more information please refer to 
https://www.postgresql.org/docs/9.5/static/sql-insert.html .

    '''.format(
            prog_name = ctx.info_name or '-unknown-',
            log_levels = ', '.join(commons.loglevel_names())
    )
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
                'schema_name': 'public',
                'on_conflict': 'do nothing'
            }],
            'topic_b': [{
                'handler': 'postgisinsert',
                'table_name': 'mytable',
                'schema_name': 'public',
                'metafield_map': { 
                    # Stores meta-field in db columns. 
                    # The metafield names are the keys, the db columns the values.
                    'filename': 'id'
                },
                'property_map': { 
                    # Setting this disables auto-mapping.
                    # manual field to db-column correlation.
                    'area_km2': 'area_km2',
                    'datetime': 'datetime'
                },
                'predefined_values': {
                    # Adds fixed values for columns.
                    # maps the names of db columns to values they will
                    # receive
                    'my_column': 'some text'
                }
            }]
        }
    })
    for line in explanation.split('\n'):
        click.echo("# {0}".format(line))
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
              help='Print an example configuration with explanations and exit.')
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

                predefined_values = config.get(['topics', topic_name, i, 'predefined_values'], required=False)
                if predefined_values is not None:
                    handler.set_predefined_values(predefined_values)

                on_conflict = config.get(['topics', topic_name, i, 'on_conflict'], required=False)
                on_conflict = on_conflict.strip()
                if on_conflict:
                    handler.on_conflict(on_conflict)
            else:
                raise ValueError('unknown handler {0}'.format(handler_name))
            if handler:
                consumer.register_topic_handler(topic_name, handler)

    click.echo('Starting to consume')
    consumer.consume()
