# encoding: utf8

from psycopg2 import Binary

#import datetime
import logging

from .base import BaseMessageHandler

logger = logging.getLogger(__name__)

# typemap to map python types to postgresql types
# TODO: cast functions ??
#TypeMap = {
#    int: "integer",
#    float: "double precision",
#    str: "text",
#    datetime.datetime: "timestamp"
#    datetime.date: "date",
#    datetime.time: "time"
#}


class PgBaseMessageHandler(BaseMessageHandler):
    '''common functionality for postgresql-targeting message handlers
       
       This class provides no handling bit itself'''

    _ident_quoted_cache = {}

    def quote_ident(self, s, cur):
        '''quote/escape an sql identifier.
           caches the results.'''
        try:
            return self._ident_quoted_cache[s]
        except KeyError:
            cur.execute('select quote_ident(%s)', (s,))
            self._ident_quoted_cache[s] = cur.fetchone()[0]
        return self._ident_quoted_cache[s]



class PostgisInsertMessageHandler(PgBaseMessageHandler):
    '''insert incomming messages in an database table.

    performs some schema introspection to find common attributes between
    the incomming data and the target tables columns + adds typecasts.
    '''

    table_name = None
    schema_name = None

    # database schema
    _columns = {}
    _geometry_column = None

    def __init__(self, schema_name, table_name):
        self.schema_name = schema_name or 'public'
        self.table_name = table_name

    def collect_schema(self, cur):
        """analyze the postgresql schema for the colums of the target table"""
        self._columns = {}
        self._geometry_column = None
        cur.execute('''select column_name, udt_name as datatype from information_schema.columns 
                            where table_name = %s
                                and table_schema = %s
                                and ordinal_position > 0''',
                            (self.table_name, self.schema_name))
        for column_name, datatype in cur.fetchall():
            if datatype == 'geometry':
                if self._geometry_column is not None:
                    raise RuntimeError('unable to deal with more than one geometry column') # needs to be improved in the future
                self._geometry_column = column_name
            else:
                self._columns[column_name] = {'datatype': datatype}

    def column_for_property(self, property_name):
        '''returns the name of the column where a property of a message should be stored.
           returning None means the property will be ignored.'''
        sanitized_name = property_name.lower() # TODO: improve
        if sanitized_name in self._columns:
           return sanitized_name
        return None

    def handle_message(self, cur, data):

        # prepare the values for insertion into the db
        target_columns = []
        values = []
        placeholders = []
        for prop_name, prop_value in data['properties'].items():
            col_name = self.column_for_property(prop_name)
            if col_name:
                target_columns.append(col_name)
                values.append(self.sanitize_value(col_name, prop_value))
                placeholders.append('%s::'+self._columns[col_name]['datatype'])
        if self._geometry_column:
            target_columns.append(self._geometry_column)
            values.append(Binary(data['wkb']))
            placeholders.append('st_geomfromwkb(%s::bytea)')

        # without any matching colums, there is nothing to do
        if len(target_columns) == 0:
            return

        sql = 'insert into {0}.{1} ({2}) select {3}'.format(
                        self.quote_ident(self.schema_name, cur),
                        self.quote_ident(self.table_name, cur),
                        ', '.join([self.quote_ident(c, cur) for c in target_columns]),
                        ', '.join(placeholders)
        )
        cur.execute(sql, values)
