# encoding: utf8
'''
Fiona bindings

-------------
Serialization
-------------

Example:

.. code-block: python

    import fiona
    from ukis_kafka.wireformat.fiona import feature_to_wireformat

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
    from ukis_kafka.wireformat.fiona import wireformat_to_feature

    # read s serialized feature from disk
    with open('searialized-feature.bin', 'r') as fh:
        feature = wireformat_to_feature(fh.read())
        # ... do something with the fiona feature
'''


from .basic import basic_to_wireformat, basic_from_wireformat

from shapely.geometry import shape, mapping
from shapely import wkb

def feature_to_wireformat(f, **kw):
    return basic_to_wireformat(
            wkb.dumps(shape(f['geometry'])), # geometry as WKB
            f.get('properties', {}),
            **kw
    )

def wireformat_to_shapely(wiredata):
    '''convert the wireformat represenation to shapely.
       returns a shapely geometry and a dictionary with the properties and a meta dict'''
    rep = basic_from_wireformat(wiredata)
    shape = None
    if rep['wkb']:
        shape = wkb.loads(rep['wkb'])
    return shape, rep['properties']


def wireformat_to_fiona(wiredata):
    shape, props = wireformat_to_shapely(wiredata)
    return {
        'properties': props,
        'geometry': mapping(shape)
    }
