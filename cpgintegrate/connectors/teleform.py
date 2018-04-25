import pandas
import sqlalchemy
from .connector import Connector
import cpgintegrate


class Teleform(Connector):

    def __init__(self, auth=("", ""), schema="postgres", port=5432, host='localhost', **kwargs):
        super().__init__(**kwargs)
        self.dbString = 'postgresql://%s:%s@%s:%s/%s' % (auth[0], auth[1], host, port, schema)

    def get_dataset(self, tbl, index_col='StudyID'):
        engine = sqlalchemy.create_engine(self.dbString)

        frame = pandas.read_sql_table(tbl, engine, parse_dates={}, coerce_float=False)

        return (frame.set_index(index_col)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: engine.url.__to_string__(True)+'/'+tbl})
                .rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME))
