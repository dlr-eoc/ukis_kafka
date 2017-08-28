# encoding: utf8

from psycopg2 import Binary
from psycopg2.extras import Json
from dateutil.parser import parse as date_parse

#import datetime
import logging

from .base import BaseMessageHandler

logger = logging.getLogger(__name__)


def pg_sanitize_value(value, pg_datatype, max_length):
    '''attempt to sanitze the value to be better suitable to
       cast to the desired datatype in postgres.
       
       in case of failures to parse the value it gets returned as it is'''
    if value is not None:
        if pg_datatype in ('date', 'timestamptz', 'timestamp'):
            try:
                return date_parse(value).isoformat()
            except:
                pass # let postgresql try its best at parsing :(
        elif pg_datatype in ('char', 'text', 'varchar'):
            # truncate texts when there is an charater limit in the db. Cast to string
            # to make in work for values send as int/float/...
            if max_length is not None:
                return str(value)[:max_length]
        elif pg_datatype == 'bytea':
            return Binary(value)
        elif pg_datatype == 'json':
            # serialize to json to use value with postgresql json type
            return Json(value)
    return value


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

    def postgresql_version(self, cur):
        cur.execute('select version()')
        return cur.fetchone()[0]

    def postgis_version(self, cur):
        cur.execute('''select exists(select routine_name from information_schema.routines where routine_name like 'postgis_version')''')
        if not cur.fetchone()[0]:
            raise RuntimeError("the database server does not have postgis installed")
        cur.execute('select postgis_version()')
        return cur.fetchone()[0]



class PostgisInsertMessageHandler(PgBaseMessageHandler):
    '''insert incomming messages in an database table.

    performs some schema introspection to find common attributes between
    the incomming data and the target tables columns + adds typecasts.
    Timestamps are analyzed and brought into ISO-format when possible.
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
        logger.info('Database server uses PostgreSQL version "{0}" with PostGIS version "{1}"'.format(
                    self.postgresql_version(cur),
                    self.postgis_version(cur),
            ))
        logger.info('Analyzing schema of relation {0}.{1}'.format(self.schema_name, self.table_name))
        cur.execute('''select column_name, udt_name as datatype, 
                                character_maximum_length as max_length
                            from information_schema.columns 
                            where table_name = %s
                                and table_schema = %s
                                and ordinal_position > 0''',
                            (self.table_name, self.schema_name))
        for column_name, datatype, max_length in cur.fetchall():
            if datatype == 'geometry':
                if self._geometry_column is not None:
                    raise RuntimeError('unable to deal with more than one geometry column') # needs to be improved in the future
                self._geometry_column = column_name
            else:
                self._columns[column_name] = {
                    'datatype': datatype,
                    'max_length': max_length
                }

    def column_for_property(self, property_name):
        '''returns the name of the column where a property of a message should be stored.
           returning None means the property will be ignored.'''
        sanitized_name = property_name.lower() # TODO: improve
        if sanitized_name in self._columns:
           return sanitized_name
        return None

    def sanitize_value(self, name, value):
        return pg_sanitize_value(value,
                    self._columns[name]['datatype'],
                    self._columns[name]['max_length']
        )

    def handle_message(self, cur, data):

        # prepare the values for insertion into the db
        target_columns = []
        values = []
        placeholders = []
        for prop_name, prop_value in data['properties'].items(): # TODO: add meta
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
