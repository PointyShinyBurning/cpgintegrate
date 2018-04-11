import pandas
import cpgintegrate.processors.utils
import pytest


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
