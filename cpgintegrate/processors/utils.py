from typing import Callable
import pandas
import re
import uuid


def match_indices(match_from: pandas.DataFrame, match_in: pandas.DataFrame,
                  index_transform: Callable = lambda x: int(re.compile(r'[^\d]+').sub("", x))) -> pandas.DataFrame:
    """
    Replace index values in a frame by searching using transformed index in another

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


def edit_using(frame_to_edit: pandas.DataFrame, edits: pandas.DataFrame) -> pandas.DataFrame:
    """
    Edit DataFrame using list of edits in another DataFrame.

    Alters each target_field value in frame_to_edit to target_value where match_field == match_value
    :param frame_to_edit: DataFrame to edit
    :param edits: DataFrame with edits in columns match_field, match_value, target_field, target_value, comment
    :return:
    """
    for _, row in edits.iterrows():
        try:
            frame_to_edit.loc[frame_to_edit[row.match_field] == row.match_value, row.target_field] \
                = row.get("target_value", None)
        except KeyError:
            print("Edits error on %s , %s" % (row.field, row.value))
    return frame_to_edit
