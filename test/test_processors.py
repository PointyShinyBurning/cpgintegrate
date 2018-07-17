import pandas
import cpgintegrate.processors.utils
import pytest
import io
from cpgintegrate.processors.extension_check import ExtensionCheck


def test_epiq7_liverelast():
    # TODO Some tests
    pass


def test_match_indices():
    assert cpgintegrate.processors.utils.match_indices(
        pandas.DataFrame(index=pandas.Index(['a123_I', '4a5b6c', '789'])),
        pandas.DataFrame(index=pandas.Index(['e123_P', 'abc456', '789', '91011']))
    ).index.equals(pandas.Index(['e123_P', 'abc456', '789']))


def test_match_indices_missing():
    with pytest.raises(AssertionError):
        cpgintegrate.processors.utils.match_indices(
            pandas.DataFrame(index=pandas.Index(['a123_I', '4a5b6c', '789', '111pol*()'])),
            pandas.DataFrame(index=pandas.Index(['e123_P', 'abc456', '789', '91011']))
        )


def test_edit_using():
    assert cpgintegrate.processors.utils.edit_using(
        pandas.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
        pandas.DataFrame({"match_field": ["A", "B"],
                          "match_value": [1, 5],
                          "target_field": ["B", "A"],
                          "target_value": [100, 500]})
    ).equals(pandas.DataFrame({"A": [1, 500, 3], "B": [100, 5, 6]}))


def test_replace_indices():
    assert cpgintegrate.processors.utils.replace_indices(
        pandas.DataFrame(index=pandas.Index(['a', 'b', 'c'])),
        pandas.DataFrame({'old_index': ['a', 'not_b', 'c']}, index=pandas.Index(['a1', 'b1', 'c1']))
    ).index.equals(pandas.Index(['a1', 'b', 'c1']))


def test_imagej_hri():
    from cpgintegrate.processors.imagej_hri import to_frame
    df = to_frame(open('res/imageJ_hri.xls', 'rb'))
    assert len(df) == 1 and df.iloc[0].Liver_Width == 1.5


def test_extension_check():
    file = io.BytesIO(b'000')
    file.name = 'file.extension'
    df = ExtensionCheck('.extension').to_frame(file)


def test_extension_check_list():
    file = io.BytesIO(b'000')
    file.name = 'file.another'
    df = ExtensionCheck(['.one', '.another']).to_frame(file)


def test_extension_check_bad():
    file = io.BytesIO(b'000')
    file.name = 'file.nothing'
    with pytest.raises(NameError):
        df = ExtensionCheck('.extension').to_frame(file)


def test_extension_check_list_bad():
    file = io.BytesIO(b'000')
    file.name = 'file.another'
    with pytest.raises(NameError):
        df = ExtensionCheck(['.one', '.two']).to_frame(file)
