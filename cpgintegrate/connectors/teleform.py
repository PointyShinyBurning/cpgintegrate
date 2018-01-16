import pandas
import sqlalchemy
from .connector import Connector


class Teleform(Connector):

    def __init__(self, docs_path, auth=("", ""), schema="postgres", port=5432, host='localhost', **kwargs):
        super().__init__(**kwargs)
        self.dbString = 'postgresql://%s:%s@%s:%s/%s' % (auth[0], auth[1], host, port, schema)
        self.docsPath = docs_path

    def _read_dataset(self, tbl,):
        engine = sqlalchemy.create_engine(self.dbString)

        frame = pandas.read_sql_table(tbl, engine, parse_dates={}, coerce_float=False)

        edits = pandas.read_csv(self.docsPath + '/' + tbl + "_edits.csv")
        print("Found edit file for " + tbl)
        for _, row in edits.iterrows():
            try:
                frame[row.field] = frame[row.field].astype(str)
                frame.loc[frame.CSID == row.CSID, row.field] = row.value
            except KeyError:
                print("Teleform edits error on %s , %s" % (row.field, row.value))

        frame.index = frame[tbl].StudyID
        return frame
