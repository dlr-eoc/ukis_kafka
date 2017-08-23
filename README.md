# UKIS Streaming

This package implements a compact binary wireformat to stream vector features over messaging networks. The package itself uses a high/easy-to-use abstraction level of the geodata mostly based on the [fiona library](http://toblerity.org/fiona/index.html).

## Library support

The library offers interfaces to a few python geodata libraries. When a new interface is needed, please try to implement it in this library for a better reuseability.

### Fiona

Fiona support is in the `ukis_streaming.fiona` module.

See

```
from ukis_streaming import fiona
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

