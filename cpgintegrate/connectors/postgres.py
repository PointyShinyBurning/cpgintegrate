import pandas
import sqlalchemy
from .connector import Connector
import cpgintegrate


class Postgres(Connector):

    def __init__(self, auth=("", ""), schema="postgres", port=5432, host='localhost', **kwargs):
        super().__init__(**kwargs)
        self.dbString = 'postgresql://%s:%s@%s:%s/%s' % (auth[0], auth[1], host, port, schema)

    def get_dataset(self, tbl, index_col='StudyID', index_col_is_subject_id=True):
        engine = sqlalchemy.create_engine(self.dbString)

        frame = pandas.read_sql_table(tbl, engine, parse_dates={}, coerce_float=False)

        return (frame.pipe(lambda df: df.set_index(index_col) if index_col else df)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: engine.url.__to_string__(True)+'/'+tbl})
                .pipe(lambda df:
                df.rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME) if index_col and index_col_is_subject_id else df)
                )
