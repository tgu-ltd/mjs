import os
import sqlite3
import logging
from mjs.config import Config


class DbTables(object):

    def __init__(self, **kwargs):
        ''' A class to extract a table from one database to another database'''

        self.rows = []
        self.table_schema = ''
        self.logger = logging.getLogger()
        config = Config(**kwargs).get()
        self.to_db = config.get('to_db', None)
        self.from_db = config.get('from_db', None)
        self.to_table = config.get('to_table', None)
        self.from_table = config.get('from_table', None)
        rm_to_db = config.get('rm_to_db', False)

        if self.to_table is None:
            self.to_table = self.from_table
        if rm_to_db:
            try:
                os.remove(self.to_db)
            except FileNotFoundError:
                pass

    def extract(self):
        self.read()
        self.write_extract()

    def append(self):
        self.read()
        self.write_append()

    def _get_table_schema(self, **kwargs):
        ''' Return a a table schema '''
        schema = ''
        db = kwargs.get('db', None)
        table = kwargs.get('table', None)
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        sql = "select sql from sqlite_master where type='table'"
        sql = "{0} and name = '{1}'".format(sql, table)
        cur.execute(sql)
        result = cur.fetchone()
        if result is None or len(result) < 1:
            msg = 'Could not find table {0} in {1} database'.format(
                db, table
            )
            self.logger.error(msg)
            raise ValueError(msg)
        schema = result[0]
        cur.close()
        con.close()
        return schema

    def read(self):
        ''' Read all the rows from the from database and table '''
        self.table_schema = self._get_table_schema(
            db=self.from_db,
            table=self.from_table
        )
        con = sqlite3.connect(self.from_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM {0}".format(self.from_table))
        for row in cur.fetchall():
            self.rows.append(dict(row))
        cur.close()
        con.close()

    def write_extract(self):
        ''' Write all the fows to the to database and table '''
        con = sqlite3.connect(self.to_db)
        cur = con.cursor()
        try:
            cur.execute(self.table_schema)
            con.commit()
        except sqlite3.OperationalError:
            pass
        for row in self.rows:
            try:
                keys, values = zip(*row.items())
                sql = "INSERT INTO {0} ({1}) values ({2})".format(
                    self.to_table,
                    ",".join(keys),
                    ",".join(['?'] * len(keys))
                )
                cur.execute(sql, values)
            except sqlite3.OperationalError as e:
                self.logger.error(str(e))
                raise ValueError(str(e))
        con.commit()
        cur.close()
        con.close()

    def _schema_to_lists(self, schema):
        ''' Remove sql from schema and return a list of values '''
        schema_list = {
            'valid': [],
            'invalid': []
        }

        schema = schema[schema.index('(') + 1:len(schema) - 1]
        for item in schema.split(','):
            item = item.strip()
            if ' ' in item:
                schema_list['valid'].append(item.strip())
            else:
                schema_list['invalid'].append(item.strip())

        return schema_list

    def write_append(self):
        ''' Append one table from one db into another db and table '''
        # get the from table schema
        # get the to table schema
        # comapre the two and find out what is missing and add it to the to_table
        from_schema = self._schema_to_lists(
            self._get_table_schema(
                db=self.from_db,
                table=self.from_table
            )
        )
        to_schema = self._schema_to_lists(
            self._get_table_schema(
                db=self.to_db,
                table=self.to_table
            )
        )

        diff = set(from_schema['valid']).difference(to_schema['valid'])
        con = sqlite3.connect(self.to_db)
        cur = con.cursor()
        for item in diff:
            split = item.split()
            col = split[0]
            ctype = split[1]
            sql = 'ALTER TABLE {0} ADD COLUMN {1} {2};'.format(
                self.to_table, col, ctype
            )
            try:
                cur.execute(sql)
            except sqlite3.OperationalError as e:
                self.logger.error(str(e))
                pass
        con.commit()

        # Add the rows from one table to the other
        for row in self.rows:
            try:
                for col in from_schema['invalid']:
                    del row[col]
                keys, values = zip(*row.items())
                sql = "INSERT INTO {0} ({1}) values ({2})".format(
                    self.to_table,
                    ",".join(keys),
                    ",".join(['?'] * len(keys))
                )
                cur.execute(sql, values)
            except sqlite3.OperationalError as e:
                self.logger.error(str(e))
                pass
        con.commit()
        cur.close()
        con.close()
