# encoding: utf8

from psycopg2 import Binary
from psycopg2.extras import Json
from dateutil.parser import parse as date_parse

import logging
import collections
import pkg_resources
import re

from .base import BaseMessageHandler

logger = logging.getLogger(__name__)

re_postgresql_version = re.compile('^[a-zA-Z]+\s*([0-9\.]+)')


def pg_sanitize_value(value, pg_datatype, max_length):
    '''attempt to sanitze the value to be better suitable to
       cast to the desired datatype in postgres.
       
       in case of failures to parse the value it gets returned as it is'''
    if value is not None:
        if pg_datatype in ('date', 'timestamptz', 'timestamp'):
            try:
                return value.isoformat()
            except AttributeError:
                try:
                    return date_parse(value).isoformat()
                except:
                    pass # let postgresql try its best at parsing :(
        elif pg_datatype in ('char', 'text', 'varchar'):
            # truncate texts when there is an charater limit in the db. Cast to string
            # to make in work for values send as int/float/...
            if max_length is not None:
                return str(value)[:max_length]
        elif pg_datatype in ('bytea', 'geometry'):
            return Binary(value)
        elif pg_datatype == 'json':
            # serialize to json to use value with postgresql json type
            return Json(value)
    return value

def postgresql_version(cur):
    cur.execute('select version()')
    return cur.fetchone()[0]

def postgresql_extract_version(version_string):
    m = re_postgresql_version.search(version_string)
    return pkg_resources.parse_version(m.group(1))

def postgis_version(cur):
    cur.execute('''select exists(select routine_name from information_schema.routines where routine_name like 'postgis_version')''')
    if not cur.fetchone()[0]:
        raise RuntimeError("the database server does not have postgis installed")
    cur.execute('select postgis_version()')
    return cur.fetchone()[0]


class QuoteIdentMixin(object):

    _ident_quoted_cache = {} # possible memory leak, but sufficient for this application

    def quote_ident(self, s, cur):
        '''quote/escape an sql identifier.
           caches the results.'''
        try:
            return self._ident_quoted_cache[s]
        except KeyError:
            cur.execute('select quote_ident(%s)', (s,))
            self._ident_quoted_cache[s] = cur.fetchone()[0]
        return self._ident_quoted_cache[s]


class PgBaseMessageHandler(BaseMessageHandler):
    '''common functionality for postgresql-targeting message handlers
       
       This class provides no handling bit itself'''
    pass


ColumnSchema = collections.namedtuple('ColumnSchema', 'name, datatype, max_length')

class PostgresqlWriter(QuoteIdentMixin):
    table_name = None
    schema_name = None
    postgres_version = None
    on_conflict = None

    _columns = {}

    def __init__(self, cur, schema_name, table_name):
        self.schema_name = schema_name or 'public'
        self.table_name = table_name
        self.analyze_schema(cur)

    def columns(self, datatype=None):
        c = {}
        c.update(self._columns) # make a copy
        if datatype: # return only the columns of the given datatype
            c = {k :v for k,v in c.iteritems() if v.datatype == datatype}
        return c

    def on_conflict(self, action):
        if self.postgres_version < pkg_resources.parse_version('9.5'):
            raise Exception('conflict handling requires postgresql >= 9.5')
        if action == 'do nothing':
            self.on_conflict = 'do nothing'
        elif action == 'do update':
            self.on_conflict = 'do update'
        else:
            raise Exception('unsupported on_conflict action: "{0}"'.format(action))

    def analyze_schema(self, cur):
        """analyze the postgresql schema for the columns of the target table"""
        self._columns = {}
        pg_version = postgresql_version(cur)
        logger.info('Database server uses PostgreSQL version "{0}" with PostGIS version "{1}"'.format(
                    pg_version,
                    postgis_version(cur),
            ))

        self.postgres_version = postgresql_extract_version(pg_version)
        logger.info('Analyzing schema of relation {0}.{1}'.format(self.schema_name, self.table_name))
        cur.execute('''select column_name, udt_name as datatype, 
                                character_maximum_length as max_length
                            from information_schema.columns 
                            where table_name = %s
                                and table_schema = %s
                                and ordinal_position > 0''',
                            (self.table_name, self.schema_name))

        for column_name, datatype, max_length in cur.fetchall():
            self._columns[column_name] = ColumnSchema(
                    name=column_name,
                    datatype=datatype,
                    max_length=max_length
            )

        # fail when the table does not exist or permissions prevent the access
        if len(self._columns) == 0:
            raise Exception('Could not collect schema information on relation {0}.{1}'.format(
                        self.schema_name, self.table_name))

    def write(self, cur, valuedict):
        target_columns = []
        values = []
        placeholders = []

        for column, value in valuedict.iteritems():
            if column in self._columns:
                column_schema = self._columns[column]
                target_columns.append(column)
                values.append(pg_sanitize_value(
                            value,
                            column_schema.datatype,
                            column_schema.max_length
                ))
                if column_schema.datatype == 'geometry':
                    placeholders.append('st_geomfromwkb(%s::bytea)')
                else:
                    placeholders.append('%s::'+column_schema.datatype)

        # without any matching columns, there is nothing to do
        if len(target_columns) == 0:
            return

        sql = 'insert into {0}.{1} ({2}) (select {3})'.format(
                        self.quote_ident(self.schema_name, cur),
                        self.quote_ident(self.table_name, cur),
                        ', '.join([self.quote_ident(c, cur) for c in target_columns]),
                        ', '.join(placeholders)
        )

        if self.on_conflict:
            sql = ' '.join((sql, ' on conflict ', self.on_conflict))
        cur.execute(sql, values)


    

class PostgisInsertMessageHandler(PgBaseMessageHandler):
    '''insert incomming messages in an database table.

    performs some schema introspection to find common attributes between
    the incomming data and the target tables columns + adds typecasts.
    Timestamps are analyzed and brought into ISO-format when possible.
    '''

    writer = None
    # database schema
    _geometry_column = None

    # dict which maps the properties/meta fields of the features
    # to database columns.
    # when this is None, auto-mapping is used
    _mapping_properties = None
    _mapping_metafields = None
    _predefined_values = None

    def __init__(self, cur, schema_name, table_name):
        self.writer = PostgresqlWriter(cur, schema_name, table_name)
        geom_columns = self.writer.columns(datatype='geometry').keys()
        if len(geom_columns) == 1:
            self._geometry_column = geom_columns[0]
        if len(geom_columns) > 1:
            raise RuntimeError('unable to deal with more than one geometry column') # needs to be improved in the future


    def set_property_mapping(self, mapping):
        '''map properties of the incomming features to database columns.

        Expects a dict in the Form:

            {
                'property-name': 'database column name'
            }

        When not set, properties will be auto-correlated by their names'''
        self._mapping_properties = mapping

    def set_metafield_mapping(self, mapping):
        '''map the meta fields of the incomming features to database columns.'''
        self._mapping_metafields = mapping

    def set_predefined_values(self, valuedict):
        self._predefined_values = valuedict

    def _column_for_property(self, property_name):
        '''returns the name of the column where a property of a message should be stored.
           returning None means the property will be ignored.'''
        if self._mapping_properties is not None:
            return self._mapping_properties.get(property_name)
        else:
            # automapping by corellating the names
            sanitized_name = property_name.lower()
            if sanitized_name in self.writer.columns():
               return sanitized_name
        return None

    def analyze_schema(self, cur):
        self.writer.analyze_schema(cur)

    def on_conflict(self, action):
        self.writer.on_conflict(action)

    def handle_message(self, cur, data):

        # collect the values for insertion into the db
        valuedict = {}

        # handle meta fields first, to give properties a higher priority (possible override)
        if self._mapping_metafields is not None:
            for mf_name, col_name in self._mapping_metafields.items():
                valuedict[col_name] = data['meta'].get(mf_name)

        for prop_name, prop_value in data['properties'].items():
            col_name = self._column_for_property(prop_name)
            if col_name:
                valuedict[col_name] = prop_value

        if self._predefined_values is not None:
            valuedict.update(self._predefined_values)

        if self._geometry_column:
            valuedict[self._geometry_column] = data['wkb']

        self.writer.write(cur, valuedict)
