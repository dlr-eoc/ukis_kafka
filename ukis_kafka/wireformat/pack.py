#encoding: utf8
'''
De-/Serialization functions
'''

import msgpack

pack = msgpack.packb

def unpack(*a, **kw):
    try:
        return msgpack.unpackb(*a, **kw)
    except Exception:
        return None
