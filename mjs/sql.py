import time
import sqlite3
import logging

'''
https://docs.python.org/2/library/sqlite3.html
https://stackoverflow.com/questions/82875/how-to-list-the-tables-in-a-sqlite-database-file-that-was-opened-with-attach
https://likegeeks.com/python-sqlite3-tutorial/
'''


class Sql(object):

    def __init__(self, **kwargs):
        self.logger = logging.getLogger()
        config = kwargs.get('config')
        if config is None:
            msg = 'No config passed as argument'
            print(msg)
            self.logger.info(msg)
            raise Exception(msg)

        self.con = None
        self.cur = None
        self._tables = {}
        self.dbfile = config.get('dbfile')

        msg = "Loading database {0}".format(self.dbfile)
        self.logger.info(msg)
        if not config.get('logtocon', True):
            print(msg)

        # On startup cache tables
        with self:
            self._cache_tables()

    def __enter__(self):
        self.con = sqlite3.connect(self.dbfile)
        self.cur = self.con.cursor()

    def __exit__(self, *args):
        self.con.close()

    def _cache_tables(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for name in self.cur.fetchall():
            name = name[0]
            if name not in self._tables:
                self._cache_table(name)
        self.logger.info("Tables found : {0}".format(len(self._tables)))

    def _cache_table(self, name):
        self.logger.info("Caching table")
        self.cur.execute("pragma table_info({0});".format(name))
        self._tables[name] = [x[1] for x in self.cur.fetchall()]

    def __get_column_type(self, value):
        ftype = "TEXT"
        if type(value) == float:
            ftype = "REAL"
        elif type(value) == int:
            ftype = "INTEGER"
        return ftype

    def _create_table(self, tablename, columns):
        self.logger.debug("Create Table {0}".format(tablename))
        keys = []
        types =[]
        # Gather column information
        for key in columns:
            ftype = self.__get_column_type(columns[key])
            keys.append(key)
            types.append(ftype)

        # To test table alter
        # keys = keys[:-1]
        # types = types[:-1]

        # Create the table creation string
        sql_columns = ''
        for key, ftype in zip(keys, types):
            sql = '{0} {1},'.format(key, ftype)
            sql_columns = sql_columns + sql
        sql_columns = sql_columns[:-1]

        if len(sql_columns) < 1:
            msg = "No columns to create table. Is the mqtt message in json format?"
            self.logger.error(msg)
            raise ValueError(msg)

        sql = 'CREATE TABLE {0} (_ts REAL PRIMARY KEY,'.format(tablename)
        sql = sql + sql_columns + ');'
        self.logger.debug(sql)
        self.cur.execute(sql)
        self.con.commit()

    def _alter_table(self, tablename, data):
        # Do we need to alter the table?
        # We want to check to see if the table contains all the columns we have
        altered = False
        for key in data:
            if key not in self._tables[tablename]:
                self.logger.debug("Altering table {0}".format(tablename))
                ftype = self.__get_column_type(data[key])
                sql = 'ALTER TABLE {0} ADD COLUMN {1} {2};'.format(
                    tablename,
                    key,
                    ftype
                )
                self.cur.execute(sql)
                self.con.commit()
                altered = True
        return altered

    def __clean_msg_keys(self, msg):
        # Remove bad key chars from the msg
        data = {}
        for key in msg:
            k = key.replace('#', '').replace('%', '')
            k = k.replace("'", '').replace('"', '')
            k = k.replace('$', '').replace('!', '')
            k = k.replace('&', '').replace('*', '')
            try:
                data[k] = msg[key]
            except TypeError as e:
                self.logger.exception("TypeError: {0}, {1}, {2}".format(e, msg, k))
                pass
        return data

    def save(self, tablename, msg):
        # Check the table exists. If not create it
        # If it does check to see if it needs altering
        # Once the checks have been done insert the data

        data = self.__clean_msg_keys(msg)
        table = self._tables.get(tablename)
        if table is None:
            self._create_table(tablename, data)
            self._cache_table(tablename)
        else:
            # Make sure that we have all the columns. Alter table if not
            if self._alter_table(tablename, data):
                self._cache_table(tablename)

        # Finally insert the data and commit
        self._insert(tablename, data)

    def _insert(self, tablename, data):
        # Have to commit the data
        sql_cols = ''
        sql_vals = ' VALUES ('
        sql = 'INSERT INTO {0} ('.format(tablename)
        data['_ts'] = time.time()
        for key in data:
            sql_cols = sql_cols + '{0},'.format(key)
            if type(data[key]) == str:
                sql_vals = sql_vals + "'{0}',".format(data[key])
            else:
                sql_vals = sql_vals + "{0},".format(data[key])

        sql_cols = sql_cols[:-1] + ')'
        sql_vals = sql_vals[:-1] + ');'
        sql = sql + sql_cols + sql_vals
        self.logger.debug(sql)
        self.logger.info("{0} data saved".format(tablename))
        self.cur.execute(sql)
        self.con.commit()
