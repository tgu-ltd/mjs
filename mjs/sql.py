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
            if '_ts' not in key:
                keys.append(key)
                types.append(ftype)

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
                self.logger.info("Altering table {0}".format(tablename))
                ftype = self.__get_column_type(data[key])
                sql = 'ALTER TABLE {0} ADD COLUMN {1} {2};'.format(
                    tablename,
                    key,
                    ftype
                )
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    altered = True
                except (ValueError, sqlite3.OperationalError, sqlite3.IntegrityError) as e:
                    msg = "Alter Table Error: {0}\n {1}".format(str(e), sql)
                    self.logger.warning(msg)
        return altered

    def _is_key_sqlable(self, char):
        rtn = False
        c = ord(char)
        if ((c >= 65) and (c <= 90)) or ((c >= 97) and (c <= 122)):
            rtn = char
        return rtn
 
    def _is_value_sqlable(self, char):
        rtn = False
        c = ord(char)
        if (c >= 40) and (c <= 122):
            rtn = char
        return rtn

    def __clean_msg_data(self, msg):
        # Remove chars that upset sql from the values and keys
        data = {}
        for k, v in msg.items():
            value = v
            key = "".join(i for i in k if self._is_key_sqlable(i))
            if isinstance(v, str):
                value = "".join(i for i in v if self._is_value_sqlable(i))

            if len(key) < 1:
                raise ValueError('Not able to make column name out of data')

            data[key] = value
        return data

    def save(self, tablename, msg):
        # Check the table exists. If not create it
        # If it does check to see if it needs altering
        # Once the checks have been done insert the data
        data = {}
        try:
            data = self.__clean_msg_data(msg)
        except ValueError:
            return
        if len(data) < 1:
            return

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
        if data.get('_ts', None) is None:
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
        try:
            self.cur.execute(sql)
            self.con.commit()
        except (ValueError, sqlite3.IntegrityError, sqlite3.OperationalError) as e:
            msg = "Insert row Error: {0}\n {1}".format(str(e), sql)
            self.logger.warning(msg)
        self.logger.debug("{0} data saved".format(tablename))
