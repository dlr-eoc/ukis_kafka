#!/bin/bash

set -e


cat >README.md <<EOF
# UKIS Kafka

This package implements a compact binary wireformat to stream vector features over Apache Kafka. The package itself uses a high/easy-to-use abstraction level of the geodata mostly based on the [fiona library](http://toblerity.org/fiona/index.html).

## Serialization format

The serialization format takes inspirations from the [data model of the fiona library](http://toblerity.org/fiona/manual.html#data-model), which
itself is based on the GeoJSON specification. This library adds a few modification to this:

* Geometries are serialized to [WKB/Well-known-binary](https://en.wikipedia.org/wiki/Well-known_text) for a more compact representation. The value is stored in the \`wkb\`-field.
* A \`meta\` object is added. This object is used to add meta information to the features without name-conflicts in the \`property\`-object.

An example for structure for a feature is

\`\`\`
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
\`\`\`

These structures are serialized using [messagepack](http://msgpack.org/).

## Shell commands

This package provides several shell commands. Each of these has its own help, which can be invoked by \`[command] --help\`.

Available after installation are:

### ukis_vectorlayer_producer

The help-text for this command:

\`\`\`
EOF
ukis_vectorlayer_producer --help >>README.md

cat >>README.md <<EOF
\`\`\`

### ukis_postgis_consumer
    
The help-text for this command:

\`\`\`
EOF
ukis_postgis_consumer --help >>README.md
cat >>README.md <<EOF
\`\`\`

A documented example configuration file:

\`\`\`
EOF
ukis_postgis_consumer --example_configuration >>README.md
cat >>README.md <<EOF
\`\`\`

### ukis_dump_consumer

The help-text for this command:

\`\`\`
EOF
ukis_dump_consumer --help >>README.md

cat >>README.md <<EOF
\`\`\`

## Library support

The library offers interfaces to a few python geodata libraries. When a new interface is needed, please try to implement it in this library for a better reuseability.

### Fiona

Fiona support is in the \`ukis_streaming.fiona\` module.

See

\`\`\`
from ukis_kafka.wireformat import fiona
help(fiona)
\`\`\`

for the documentation.

### Shapely

There is some support in the \`ukis_streaming.fiona\` module.

### OGR

Support for the native OGR library bindings still needs to be implemented.


# TODO


* SRID support
* OGR support
EOF
