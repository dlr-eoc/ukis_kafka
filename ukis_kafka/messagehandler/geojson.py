# encoding: utf8

from .base import BaseMessageHandler

from shapely.geometry import mapping
from shapely import wkb

import json

class GeoJsonFileHandler(BaseMessageHandler):
    '''write incomming features to a geojson file
       as geojson features'''

    _fh = None
    seperator = None

    def __init__(self, f, seperator=',\n'):
        self.seperator = seperator
        if hasattr(f, 'write'):
            self._fh = f
        else:
            self._fh = open(f, 'w')

    def handle_message(self, data):
        feature = {
            'type': 'Feature',
            'properties': data['properties'],
            'meta': data['meta']
        }
        if data.get('wkb'):
            shp = wkb.loads(data['wkb'])
            feature['geometry'] = mapping(shp)

        if self.seperator is not None:
            self._fh.write(',\n')
        self._fh.write(json.dumps(feature))
        if hasattr(self._fh, 'flush'):
            self._fh.flush()
        return True
