from abc import ABC, abstractmethod
import typing
import pandas
import cpgintegrate


class Connector(ABC):

    def __init__(self, **kwargs):
        pass

    def get_dataset(self, *args, **kwargs) -> pandas.DataFrame:
        return (self
                ._read_dataset(*args, **kwargs)
                .rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME)
                )

    @abstractmethod
    def _read_dataset(self, *args, **kwargs) -> pandas.DataFrame:
        pass


class FileDownloadingConnector(Connector):

    @abstractmethod
    def iter_files(self, *args, **kwargs) -> typing.Iterator[typing.IO]:
        pass

    def process_files(self, processor, *args, **kwargs):
        return cpgintegrate.process_files(self.iter_files(*args, **kwargs), processor)
