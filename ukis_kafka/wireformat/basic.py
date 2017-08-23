# encoding: utf8

from . import pack

def basic_to_wireformat(wkb, properties, meta={}):
    '''convert basic values to the wireoformat representation'''
    properties = properties or {}
    meta = meta or {}
    return pack.pack({
        'wkb': wkb,
        'meta': meta,
        'properties': properties,
    })

def basic_from_wireformat(wiredata):
    '''convert the wireformat data to basic values'''
    rep = pack.unpack(wiredata)
    if not type(rep) == dict:
        raise ValueError('expected a dict in the binary data')

    # sanitze output a bit so all expected attributes exist regardless
    # of the input data
    if not 'wkb' in rep:
        rep['wkb'] = None
    for a in ('meta', 'properties'):
        if not a in rep:
            rep[a] = {}

    return rep
