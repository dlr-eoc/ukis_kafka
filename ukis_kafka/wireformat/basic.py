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

def _dict_to_utf8(d, no_conversion_keys=[]):
    new_d = {}
    for k,v in d.items():
        new_k = k
        new_v = v
        if type(k) == bytes:
            new_k = k.decode('utf-8')
        if new_k not in no_conversion_keys:
            if type(v) == bytes:
                new_v = v.decode('utf-8')
            elif type(v) == dict:
                new_v = _dict_to_utf8(v)
        new_d[new_k] = new_v
    return new_d

def basic_from_wireformat(wiredata):
    '''convert the wireformat data to basic values'''
    rep = pack.unpack(wiredata)
    if not type(rep) == dict:
        raise ValueError('expected a dict in the binary data')

    # convert to utf8, except the wkb string
    rep = _dict_to_utf8(rep, no_conversion_keys=['wkb'])

    # sanitze output a bit so all expected attributes exist regardless
    # of the input data
    if not 'wkb' in rep:
        rep['wkb'] = None
    for a in ('meta', 'properties'):
        if not a in rep:
            rep[a] = {}

    return rep
