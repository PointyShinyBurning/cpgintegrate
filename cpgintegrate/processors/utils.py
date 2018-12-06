import uuid
from typing import Callable
import pandas
import re
from cpgintegrate import ProcessingException


def match_indices(match_from: pandas.DataFrame, match_in: pandas.DataFrame,
                  index_transform: Callable = lambda x: int(re.compile(r'[^\d]+').sub("", x))) -> pandas.DataFrame:
    """
    Replace index values in a frame by searching using transformed index in another

    Replace indices of match_from with first instance found in match_in when both are transformed using index_transform.
    KeyError if any index of match_from is missing.

    :param match_from: DataFrame whose index values I should check and replace
    :param match_in: DataFrame which I should search in
    :param index_transform: Callable that acts on index value, default takes numeric parts
    :return:
    """
    match_in_reindexed = (match_in
                          .drop(columns=match_in.columns)
                          .assign(orig_index=lambda df: df.index)
                          .pipe(lambda df: df.set_index(df.index.map(index_transform)))
                          # Only want one row for each transformed index to avoid multiplying rows in match_from frame
                          .groupby(level=0)
                          .first())
    matched = (
        match_from
        .assign(**{'transformed_index': lambda df: df.index.map(index_transform)})
        .join(match_in_reindexed, on='transformed_index')
    )
    assert not matched.orig_index.isnull().any(), "Bad IDs {0}".format(matched.loc[matched.orig_index.isnull(),
                                                                                   'transformed_index'])
    return match_from.set_index(matched.set_index('orig_index').rename_axis(match_from.index.name).index)


def edit_using(frame_to_edit: pandas.DataFrame, edits: pandas.DataFrame) -> pandas.DataFrame:
    """
    Edit DataFrame using list of edits in another DataFrame.

    Alters each target_field value in frame_to_edit to target_value where match_field == match_value, or removes the row
    if remove_row is > 0
    :param frame_to_edit: DataFrame to edit
    :param edits: DataFrame with edits in columns match_field, match_value, target_field, target_value
    optionally remove_row
    :return:
    """

    frame_to_edit = frame_to_edit.copy(deep=True)

    # remove_rows
    if "remove_row" in edits.columns:
        for _, remove_spec in edits[edits.remove_row > 0].iterrows():
            frame_to_edit = frame_to_edit[~(frame_to_edit[remove_spec.match_field] == remove_spec.match_value)]

    # Use temporary name to avoid clobbering any existing column
    temp_col_name = None
    orig_index_name = None
    if frame_to_edit.index.name and (frame_to_edit.index.name in edits.target_field.values):
        temp_col_name = str(uuid.uuid4())
        orig_index_name = frame_to_edit.index.name
        frame_to_edit[temp_col_name] = frame_to_edit.index
        edits.loc[edits.target_field == orig_index_name, 'target_field'] = temp_col_name

    for _, row in edits.loc[edits.target_field.fillna("") != ""].iterrows():
        print(frame_to_edit)
        try:
            frame_to_edit.loc[frame_to_edit[row.match_field] == row.match_value, row.target_field] \
                = row.get("target_value", None)
        except KeyError:
            raise ProcessingException("Edits error on %s , %s, %s, %s"
                                      % (row.match_field, row.match_value,
                                         row.target_field, row.get("target_value", None)))

    if temp_col_name:
        return frame_to_edit.set_index(temp_col_name).rename_axis(orig_index_name)
    return frame_to_edit


def replace_indices(target: pandas.DataFrame, index_source: pandas.DataFrame) -> pandas.DataFrame:
    return target.set_index(
        target.index.to_series().replace(to_replace=list(index_source.iloc[:, 0]), value=list(index_source.index))
    )
