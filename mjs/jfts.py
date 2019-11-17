import glob
import json
from mjs.sql import Sql
from mjs.config import Config


class Jfts(object):

    def __init__(self, **kwargs):
        ''' Convert JSON text files to sqlite db
            Files should contain lines of json i.e, {"key": "value"}\n{"key": "value"}
        '''

        self.rows = []
        config = Config(**kwargs).get()
        self.database = Sql(config=config)
        self.tablename = config.get('tablename', 'JftsTable')
        self.use_ts_field = config.get('use_ts_field', None)
        self.add_to_row = config.get('add_to_row', 'key:none')
        self.filenames = config.get('filenames', 'None')
        self.filespath = config.get('filespath', 'None')

    def load(self):
        rows = []
        path = '{0}/{1}*'.format(self.filespath, self.filenames)
        filenames = glob.glob(path)
        if len(filenames) < 1:
            raise Exception('Could not find any files to read. Check file arguments')
        for fname in filenames:
            with open(fname, 'r') as f:
                for line in f.readlines():
                    try:
                        rows.append(json.loads(line))
                    except json.decoder.JSONDecodeError:
                        pass
        if len(rows) < 1:
            raise Exception('Could load any json data. Check data structures in files')

        self.rows = sorted(rows, key=lambda item: item[self.use_ts_field])
        print("Loaded {0} files with {1} entries".format(
            len(filenames), len(self.rows)
        ))

    def save(self):
        with self.database:
            # print("{0}:{1}".format(row.get('_ts'), row.get('time')))
            for i, row in enumerate(self.rows):
                row['_ts'] = None
                if self.use_ts_field is not None:
                    row['_ts'] = int(row.get(self.use_ts_field))
                if self.add_to_row is not None:
                    split = self.add_to_row.split(':')
                    row[split[0]] = split[1]
                if not i % 1000:
                    print('Processed {0} out of {1} rows {2} % done'.format(
                        i, len(self.rows), round((i / len(self.rows) * 100), 2)
                    ))
                self.database.save(self.tablename, row)
