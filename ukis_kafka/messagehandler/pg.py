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


ColumnSchema = collections.namedtuple('ColumnSchema', 'name, datatype, max_length, srid')

class PostgresqlWriter(QuoteIdentMixin):
    table_name = None
    schema_name = None
    postgres_version = None
    on_conflict = None

    _columns = {}

    # maps unique_constraints to their columns
    _unique_constraints = collections.defaultdict(set)
    _relevant_unique_constraint_name = None

    def __init__(self, cur, schema_name, table_name):
        self.schema_name = schema_name or 'public'
        self.table_name = table_name
        self.analyze_schema(cur)

    def columns(self, datatype=None):
        c = {}
        c.update(self._columns) # make a copy
        if datatype: # return only the columns of the given datatype
            c = {k :v for k,v in c.items() if v.datatype == datatype}
        return c

    def on_conflict(self, action, conflict_constraint=None):
        '''conflict handling.
           
           action='do update' attempts to infer the updatable columns by
           the available unique constraints on the table. This will only work
           when there is only one unique constraint. In the case there are more
           than one, you can specify the name of the relevant unique constraint
           using the 'conflict_constraint' parameter. The value for this parameter
           may be the name of the constraint, or a comma-seperated list of the
           columns which are part in this constraint.'''
        if self.postgres_version < pkg_resources.parse_version('9.5'):
            raise Exception('conflict handling requires postgresql >= 9.5')
        if action == 'do nothing':
            self.on_conflict = 'do nothing'
        elif action == 'do update':
            if conflict_constraint is not None:
                if ',' in conflict_constraint:
                    # in case the value is a comma-seperated list, split it ad asume it is a list of 
                    # columns
                    con_colums = set([v.strip() for v in conflict_constraint.split(',')]) - set([''])

                    for con_name_i, con_columns_i in self._unique_constraints.items():
                        if con_columns_i == con_colums:
                            self._relevant_unique_constraint_name = con_name_i
                            break

                    if self._relevant_unique_constraint_name is None:
                        raise Exception('Could not find a matching unique constraint on table {0} containing the columns {1}'.format(
                                        self.table_name,
                                        ', '.join(list(con_colums))
                                    ))
                else:
                    if conflict_constraint not in self._unique_constraints:
                        raise Exception('The table {0} has no unique constraint with the name {1}'.format(
                                        self.table_name,
                                        conflict_constraint
                                    ))
                    self._relevant_unique_constraint_name = conflict_constraint
            else:
                if len(self._unique_constraints) == 1:
                    self._relevant_unique_constraint_name = list(self._unique_constraints.items())[0][0]
                else:
                    raise Exception('do update is only supported when there is only one unique constraint for the table {0}. You may need to specify the conflict_constraint parameter'.format(self.table_name))
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
                    max_length=max_length,
                    srid=0
            )

        # fail when the table does not exist or permissions prevent the access
        if len(self._columns) == 0:
            raise Exception('Could not collect schema information on relation {0}.{1} using information_schema.columns. Is the current user member of the role which owns the target table?'.format(
                        self.schema_name, self.table_name))

        # collect the srids of the geometry columns
        geom_column_names = [k for k, v in list(self._columns.items()) if v.datatype == 'geometry']
        if geom_column_names != []:
            cur.execute('''select f_geometry_column, srid from geometry_columns
                        where f_table_schema = %s
                            and f_table_name=%s''',
                        (self.schema_name, self.table_name))
            for column_name, srid in cur.fetchall():
                # replace the namedtuple instance
                vc = dict(self._columns[column_name]._asdict())
                vc['srid'] = srid
                self._columns[column_name] = ColumnSchema(**vc)

        # collect unique constraints
        self._unique_constraints = collections.defaultdict(set)
        cur.execute('''select ccu.column_name, tc.constraint_name
                            from information_schema.table_constraints tc
                            join information_schema.constraint_column_usage ccu on
                                ccu.table_name = tc.table_name
                                and ccu.table_schema = tc.table_schema
                                and ccu.constraint_name = tc.constraint_name
                        where tc.constraint_type = 'UNIQUE'
                            and tc.table_name = %s
                            and tc.table_schema = %s
                ''', (self.table_name, self.schema_name))
        for col_name, con_name in cur.fetchall():
            self._unique_constraints[con_name].add(col_name)

    def write(self, cur, valuedict):
        '''write a row to the database.
           
           returns a boolean indicating if the row has been inserted (true)'''
        target_columns = []
        values = []
        placeholders = []

        for column, value in valuedict.items():
            if column in self._columns:
                column_schema = self._columns[column]
                target_columns.append(column)
                values.append(pg_sanitize_value(
                            value,
                            column_schema.datatype,
                            column_schema.max_length
                ))
                if column_schema.datatype == 'geometry':
                    if column_schema.srid != 0:
                        # TODO: reprojection support
                        values.append(column_schema.srid)
                        placeholders.append('st_setsrid(st_geomfromwkb(%s::bytea), %s)')
                    else:
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
            conflict_parts = [sql,]
            if self.on_conflict == 'do update':
                con_columns = self._unique_constraints[self._relevant_unique_constraint_name]

                updateable_columns = list(set(target_columns) - con_columns)
                if updateable_columns != []:
                    conflict_parts.append('on conflict')
                    conflict_parts.append('on constraint')
                    conflict_parts.append(self.quote_ident(self._relevant_unique_constraint_name, cur))
                    conflict_parts.append('do update set')

                    uc_parts = []
                    for uc in updateable_columns:
                        uc_parts.append('{uc} = EXCLUDED.{uc}'.format(uc=self.quote_ident(uc, cur)))
                    conflict_parts.append(', '.join(uc_parts))
            elif self.on_conflict == 'do nothing':
                conflict_parts.append('on conflict')
                conflict_parts.append(self.on_conflict)
            sql = ' '.join(conflict_parts)

        # add returning clause to be able to determinate if the row
        # was actualy inserted
        sql += ' returning true';

        cur.execute(sql, values)
        row = cur.fetchone()
        if not row or row[0] == False:
            # Nothing has been inserted.
            #
            # Returning a failure here allows a rollback of the db
            # transaction outside of this class. This prevents
            # sequences attached to this table from being increased 
            # for each failure.
            # See the notes on 
            # https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT
            # "...  that the effects of all per-row BEFORE INSERT 
            # triggers are reflected in excluded values, since those effects 
            # may have contributed to the row being excluded from insertion. ..."
            return False
        return True


    

class PostgisInsertMessageHandler(PgBaseMessageHandler):
    '''insert incomming messages in an database table.

    performs some schema introspection to find common attributes between
    the incomming data and the target tables columns + adds typecasts.
    Timestamps are analyzed and brought into ISO-format when possible.
    '''

    writer = None
    # database schema
    _geometry_column = None
    _discard_geometries = False

    # dict which maps the properties/meta fields of the features
    # to database columns.
    # when this is None, auto-mapping is used
    _mapping_properties = None
    _mapping_metafields = None
    _predefined_values = None

    def __init__(self, cur, schema_name, table_name):
        self.writer = PostgresqlWriter(cur, schema_name, table_name)
        geom_columns = list(self.writer.columns(datatype='geometry').keys())
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

    def set_discard_geometries(self, discard_geometries):
        '''discard incomming geometries and do not insert them into the db.
           default: false/do not discard'''
        self._discard_geometries = discard_geometries

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

    def on_conflict(self, action, conflict_constraint=None):
        self.writer.on_conflict(action, conflict_constraint=conflict_constraint)

    def handle_message(self, cur, data):
        '''returns an boolean indicating if the message has been added
        to the database'''

        # collect the values for insertion into the db
        valuedict = {}

        # handle meta fields first, to give properties a higher priority (possible override)
        if self._mapping_metafields is not None:
            for mf_name, col_name in list(self._mapping_metafields.items()):
                valuedict[col_name] = data['meta'].get(mf_name)

        for prop_name, prop_value in list(data['properties'].items()):
            col_name = self._column_for_property(prop_name)
            if col_name:
                valuedict[col_name] = prop_value

        if self._predefined_values is not None:
            valuedict.update(self._predefined_values)

        if self._geometry_column and not self._discard_geometries:
            valuedict[self._geometry_column] = data['wkb']

        return self.writer.write(cur, valuedict)
