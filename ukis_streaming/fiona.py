# encoding: utf8
'''
Fiona bindings

-------------
Serialization
-------------

Example:

.. code-block: python

    import fiona
    from ukis_streaming.fiona import feature_to_wireformat

    with fiona.open('example.shp', 'r') as src:
        for feature in src:
            binary_repr = feature_to_wireformat(feature)
            # ... do something with the binary string


---------------
Deserialization
---------------

Example:


.. code-block: python

    import fiona
    from ukis_streaming.fiona import wireformat_to_feature

    # read s serialized feature from disk
    with open('searialized-feature.bin', 'r') as fh:
        feature = wireformat_to_feature(fh.read())
        # ... do something with the fiona feature
'''

from . import pack

from shapely.geometry import shape, mapping
from shapely import wkb

def feature_to_wireformat(f):
    rep = {
        'properties': f.get('properties', {}),

        # geometry as WKB
        'wkb': wkb.dumps(shape(f['geometry']))
    }
    return pack.pack(rep)
    

def wireformat_to_shapely(data):
    '''convert the wireformat represenation to shapely.
       returns a shapely geometry and a dictionary with the properties'''
    rep = pack.unpack(data)
    if not type(rep) == dict:
        raise ValueError('expected a dict in the binary data')
    return wkb.loads(rep['wkb']), rep.get('properties', {})


def wireformat_to_fiona(data):
    geom, props = wireformat_to_shapely(data)
    return {
        'properties': props,
        'geometry': mapping(geom)
    }
