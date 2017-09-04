# UKIS Kafka

This package implements a compact binary wireformat to stream vector features
over Apache Kafka. The package itself uses a high/easy-to-use abstraction level
of the geodata mostly based on the [fiona
library](http://toblerity.org/fiona/index.html).

## Serialization format

The serialization format takes inspirations from the [data model of the fiona
library](http://toblerity.org/fiona/manual.html#data-model), which itself is
based on the GeoJSON specification. This library adds a few modification to
this:

* Geometries are serialized to
  [WKB/Well-known-binary](https://en.wikipedia.org/wiki/Well-known_text) for a
  more compact representation. The value is stored in the `wkb`-field.

* A `meta` object is added. This object is used to add meta information to
  the features without name-conflicts in the `property`-object.

An example for structure for a feature is

```
{
    'properties': {
        'prop_1': 34,
        'prop_2': 'this is a string'
    }
    'wkb': WKB ...,
    'meta': {
        'some_text': 'some meta information'
    }
}
```

These structures are serialized using [messagepack](http://msgpack.org/).

## Shell commands

This package provides several shell commands. Each of these has its own help,
which can be invoked by `[command] --help`.

Available after installation are:

### ukis_vectorlayer_producer

The help-text for this command:

```
Usage: ukis_vectorlayer_producer [OPTIONS] FILENAME

  Read vector geodata from a file and push it into Kafka.

Options:
  --version                       Print version and exit.
  -k, --kafka_server TEXT         Hostname and port of the kafka server to
                                  connect to.  [required]
  -t, --topic TEXT                Topic under which the vectors are published.
                                  [required]
  -l, --loglevel [warn|info|warning|debug|error]
                                  Loglevel. logs will be written to stdout.
  -m, --meta_field TEXT           Add a field with metainformation to the
                                  data. This option can be used multiple
                                  times. Syntax: KEY=VALUE
  --help                          Show this message and exit.
```

### ukis_postgis_consumer
    
The help-text for this command:

```
Usage: ukis_postgis_consumer [OPTIONS] CFG_FILE

  Consume vetor data from Kafka and write it to a PostGIS database.

  Attempts to autocast incomming ettributes to the types of the database
  columns. Timestamps are analyzed and brought into ISO-format when
  possible.

  Configuration is handled by a YAML configuration file. See the
  --example_configuration switch.

Options:
  --version                Print version and exit.
  --example_configuration  Print an example configuration with explanations
                           and exit.
  --help                   Show this message and exit.
```

A documented example configuration file:

```
# 
# Configuration file for ukis_postgis_consumer
# 
# Logging section
# ===============
# 
# * The specified logfile will automaticaly be rotated.
# * When no logfile is set, the log will be written to stdout.
# * The available log levels are: warn, info, warning, debug, error.
# 
# Postgresql section
# ==================
# 
# * The 'dsn'-Parameter is mandatory and used to connect to the
#   database server. The syntax is libpqs connection string format
#   and documented under 
#   https://www.postgresql.org/docs/9.6/static/libpq-connect.html
# 
# Kafka section
# =============
# 
# * All parameters are mandatory.
# 
# Topics section
# ==============
# 
# This configuration section contains a mapping of topic names to subscribe to
# and handler chains where the received messages are passed through. This means
# it is possible to specify more than one handler to process messages multiple
# times.
# 
# Each handler configuration consists of the mandatory name of the handler in
# the "handler" parameter and a handler specific number of settings.
# 
# postgisinsert Handler
# ---------------------
# 
# This handler inserts incomming messages into the database.
# PostGIS geometries are supported. Transactions are supported, each message
# is inserted with its own savepoint. So a rollback only needs to happen for the
# last message when inserting fails. Messages which can not be inserted in the
# database will be discarded.
# 
# Incomming values are cast to the type of the database columns when possible and
# there are sanitation routines for types like timestamps to attempt to make them
# understandable for the database server.
# 
# The 'table_name' and 'schema_name' settings specify the target table and are
# required.
# 
# 'property_map' maps the properties of the features of the messages to database
# columns. The keys are the names of the features properties, the values the
# names of the db columns. When this setting is not set ukis_postgis_consumer performs an
# automapping and correlates properties and columns by their names.
# 
# 'metafield_map' maps the fields of the messages 'meta' attribute to database
# columns. The keys are the names of the features meta fields, the values the
# names of the db columns.  When not set, no mapping will be performed.
# 
# 'predefined_values' is a set of values defined in the configuration file which
# will be inserted into the database columns. The keys are the names of the
# database columns, the values are the values.
# 
# The 'on_conflict' settings is optional and supports PostgreSQLs INSERT-conflict
# handling.  Possible values are 'do nothing' and 'do update'. This setting
# requires PostgreSQL 9.5.  For more information please refer to
# https://www.postgresql.org/docs/9.5/static/sql-insert.html .
# 
# The 'discard_geometries' setting is useful when only properties and/or meta fields
# are supposed to be synced to the database. Existing geometry columns will not
# be inserted/updated. The default for this behavior is False/Off.
#     
kafka:
  client_id: my-client-id
  group_id: my-group-id
  kafka_server: localhost:9092
logging:
  file: /tmp/example.log
  level: info
postgresql:
  dsn: dbname=test user=tester password=secret
topics:
  topic_a:
  - handler: postgisinsert
    on_conflict: do nothing
    schema_name: public
    table_name: mytable
  topic_b:
  - discard_geometries: false
    handler: postgisinsert
    metafield_map:
      filename: id
    predefined_values:
      my_column: some text
    property_map:
      area_km2: area_km2
      datetime: datetime
    schema_name: public
    table_name: mytable

```

### ukis_dump_consumer

The help-text for this command:

```
Usage: ukis_dump_consumer [OPTIONS]

  Consume features from a topic and dump them to file.

Options:
  --version                       Print version and exit.
  -k, --kafka_server TEXT         Hostname and port of the kafka server to
                                  connect to.  [required]
  -t, --topic TEXT                Topic under which the vectors are published.
                                  [required]
  -c, --client_id TEXT            Client_id for this consumer.  [required]
  -g, --group_id TEXT             Group_id for this consumer.  [required]
  -f, --filename TEXT             File to dump the received features to.
                                  Default is stdout ("-")
  -l, --loglevel [warn|info|warning|debug|error]
                                  Loglevel. logs will be written to stdout.
  -F, --output_format [geojson]   Outputformat
  --help                          Show this message and exit.
```

## Library support

The library offers interfaces to a few python geodata libraries. When a new
interface is needed, please try to implement it in this library for a better
reuseability.

### Fiona

Fiona support is in the `ukis_streaming.fiona` module.

See

```
from ukis_kafka.wireformat import fiona
help(fiona)
```

for the documentation.

### Shapely

There is some support in the `ukis_streaming.fiona` module.

### OGR

Support for the native OGR library bindings still needs to be implemented.


# TODO


* SRID support
* OGR support
