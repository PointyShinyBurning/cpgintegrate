from pandas import DataFrame
import json


class ColumnInfoFrame(DataFrame):
    """
    Adds arbitrary dict of column metadata and a method to output it to ckan-datastore compliant JSON field list

    LIKELY BAD FOR GENERAL USE, only parts I've used in this package have been looked at to see if they work properly
    """

    _metadata = ['_column_info']

    def __init__(self, *args, **kwargs):
        column_info = kwargs.pop('column_info', {})
        super().__init__(*args, **kwargs)
        self._column_info = column_info

    @property
    def _constructor(self):
        return ColumnInfoFrame

    def __finalize__(self, other, method=None, **kwargs):
        """propagate metadata from other to self """
        if method == 'concat':
            for obj in other.objs:
                if hasattr(obj, 'column_info'):
                    self._column_info.update(obj.column_info)
        else:
            if hasattr(other, 'column_info'):
                self._column_info.update(other.column_info)
        return self

    def get_json_column_info(self):
        return json.dumps(self.get_column_info())

    def get_column_info(self):
        return [
            {**{"id": col_name}, **({"info": self._column_info[col_name]} if col_name in self._column_info else {})}
            for col_name in [self.index.name or ""]+list(self.columns)
        ]

    def set_column_info(self, column_info):
        copy = self.copy(deep=True)
        copy._column_info = column_info
        return copy

    def equals(self, other):
        try:
            (other.get_column_info() == self.get_column_info()) and super().equals(other)
        except AttributeError:
            return False
