import pandas
import sqlalchemy
from .connector import Connector


class Teleform(Connector):

    def __init__(self, auth=("", ""), schema="postgres", port=5432, host='localhost', **kwargs):
        super().__init__(**kwargs)
        self.dbString = 'postgresql://%s:%s@%s:%s/%s' % (auth[0], auth[1], host, port, schema)

    def _read_dataset(self, tbl,):
        engine = sqlalchemy.create_engine(self.dbString)

        frame = pandas.read_sql_table(tbl, engine, parse_dates={}, coerce_float=False)

        frame.index = frame.StudyID
        return frame
