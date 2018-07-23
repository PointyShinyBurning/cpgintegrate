from cpgintegrate.column_info_frame import ColumnInfoFrame
import pandas


def test_concat():
    assert (pandas
            .concat([ColumnInfoFrame({"a": [1, 2, 3]}, column_info={'a': {'position': '1'}}),
                     ColumnInfoFrame({"a": [4, 5, 6]})])
            .get_column_info()) == [{'id': ''}, {'id': 'a', 'info': {'position': '1'}}]


def test_apply():
    assert(ColumnInfoFrame({"a": [1, 2, 3]}, column_info={"a": {"position": '1'}})
           .apply(lambda row: row.set_value("a", 10))
           .get_column_info()) == [{'id':''}, {'id': 'a', 'info': {'position': '1'}}]