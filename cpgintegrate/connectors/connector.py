from abc import ABC, abstractmethod
import typing
import pandas
import cpgintegrate


class Connector(ABC):

    def __init__(self, **kwargs):
        # Sink for **vars(conn) and **conn.extra_dejson in cpg_airflow_plugin.py that Connector doesn't use
        pass

    @abstractmethod
    def get_dataset(self, *args, **kwargs) -> pandas.DataFrame:
        pass


class FileDownloadingConnector(Connector):

    @abstractmethod
    def iter_files(self, *args, **kwargs) -> typing.Iterator[typing.IO]:
        pass

    def process_files(self, processor, limit=None, *args, **kwargs):
        return cpgintegrate.process_files(self.iter_files(*args, **kwargs), processor, limit=limit)
