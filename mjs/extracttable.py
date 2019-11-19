import os
import sqlite3
from mjs.config import Config


class ExtractTable(object):

    def __init__(self, **kwargs):
        ''' A class to extract a table from one database to another database'''

        self.rows = []
        self.table_schema = ''
        config = Config(**kwargs).get()
        self.to_db = config.get('to_db', None)
        self.from_db = config.get('from_db', None)
        self.to_table = config.get('to_table', None)
        self.from_table = config.get('from_table', None)
        rm_to_db = config.get('rm_to_db', False)
        if self.to_table is None:
            self.to_table = self.from_table
        if rm_to_db:
            os.remove(self.to_db)

    def read(self):
        ''' Read all the rows from the from database and table '''
        con = sqlite3.connect(self.from_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        sql = "select sql from sqlite_master where type='table'"
        sql = "{0} and name = '{1}'".format(sql, self.from_table)
        cur.execute(sql)
        result = cur.fetchone()
        if result is None or len(result) < 1:
            raise ValueError('Could not find table {0} in {1} database'.format(
                self.from_db,
                self.from_table
            ))
        self.table_schema = result[0]
        cur.execute("SELECT * FROM {0}".format(self.from_table))
        for row in cur.fetchall():
            self.rows.append(dict(row))
        cur.close()
        con.close()

    def write(self):
        ''' Write all the fows to the to database and table '''
        con = sqlite3.connect(self.to_db)
        cur = con.cursor()
        cur.execute(self.table_schema)
        con.commit()
        for row in self.rows:
            keys, values = zip(*row.items())
            sql = "INSERT INTO {0} ({1}) values ({2})".format(
                self.to_table,
                ",".join(keys),
                ",".join(['?'] * len(keys))
            )
            cur.execute(sql, values)
        con.commit()
        cur.close()
        con.close()
