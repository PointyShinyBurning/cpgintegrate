from typing import Callable
import pandas
import re
import uuid


def match_indices(match_from: pandas.DataFrame, match_in: pandas.DataFrame,
                  index_transform: Callable = lambda x: int(re.compile(r'[^\d]+').sub("", x))) -> pandas.DataFrame:
    """
    Replace indices of match_from with those found in match_to when both are transformed using index_transform.
    KeyError if any index of match_from is missing.

    :param match_from: DataFrame whose index values I should check and replace
    :param match_in: DataFrame which I should search in
    :param index_transform: Callable that acts on index value, default takes numeric parts
    :return:
    """
    temp_col = str(uuid.uuid4())
    match_in_reindexed = (match_in
                          .assign(orig_index=lambda df: df.index)
                          .pipe(lambda df: df.set_index(df.index.map(index_transform))))
    matched = match_from.set_index(
        match_from
        .assign(**{temp_col: lambda df: df.index.map(index_transform)})
        .join(match_in_reindexed, on=temp_col).set_index('orig_index').index
    )
    assert not matched.index.isnull().any()
    return matched
