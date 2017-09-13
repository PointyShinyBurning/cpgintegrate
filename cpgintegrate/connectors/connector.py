from abc import ABC, abstractmethod
import typing
import pandas
import cpgintegrate


class Connector(ABC):

    @abstractmethod
    def iter_files(self, *args, **kwargs) -> typing.Iterator[typing.IO]:
        pass

    @abstractmethod
    def get_dataset(self, *args, **kwargs) -> pandas.DataFrame:
        pass

    def process_files(self, processor, *args, **kwargs):
        return cpgintegrate.process_files(self.iter_files(*args, **kwargs), processor)
