import cpgintegrate
from cpgintegrate import ColumnInfoFrame


def brackets_frame_to_col_info(sheet):
    new_col_names = [col.split('(')[0].strip() for col in sheet.columns]
    col_info = {new: {cpgintegrate.UNITS_ATTRIBUTE_NAME: old.split('(')[1][:-1]}
                for old, new in zip(sheet.columns, new_col_names) if '(' in old}
    sheet.columns = new_col_names

    return ColumnInfoFrame(sheet, column_info=col_info)
