"""
Screen-scrapes bloods results in 'The Doctors Lab' html format into a DataFrame
"""
import pandas
from bs4 import BeautifulSoup
import cpgintegrate
from cpgintegrate.column_info_frame import ColumnInfoFrame


def to_frame(file):
    html = file.read()
    soup = BeautifulSoup(html, "lxml")

    try:
        study_id = str(soup.div.tr.contents[1].string)
    except AttributeError:
        return None

    # Read in table, drop blanks, label columns
    tables = pandas.read_html(html, parse_dates=False)
    data = pandas.concat(tables[1:])
    data = data[(data[1] == data[1]) & (data[0] == data[0])]
    data.rename(columns={0: 'field', 1: 'value', 2: 'units', 3: 'range'}, inplace=True)

    # Convert values to strings now that "infer_types" doesn't do anything
    data.value = [str(v) for v in data.value]

    # Mark outofrange and take out the star
    data['outofrange'] = float('NaN')
    data.loc[["*" in v for v in data['value']] & (data['range'] == data['range']),
             ['outofrange']] = 1
    data['value'] = [value.replace("*", "") for value in data['value']]

    # Units to Column Info
    with_units = (data['units'] == data['units'])
    col_info = {col_name: {cpgintegrate.UNITS_ATTRIBUTE_NAME: units} for col_name, units
                in data.ix[with_units, ['field','units']].values}
    # f = Blood_results.decode_results(s.read())

    # Fill with "nan" to match missing columns and drop valueless columns
    data.fillna("nan", inplace=True)
    data = data[data.value != "nan"]

    # Drop any fields which are actually comments and save for later
    comments = {"field": "comment",
                "value": "\n".join(data[(data['field'] == "nan") | (data['units'] == 'nan')]["value"])}
    data = data[(data['field'] != "nan") & (data['units'] != 'nan')]

    # Create extra OutofRange column
    data_out_range = data.copy()
    data_out_range.drop(['value', 'units', 'range'], inplace=True, axis=1)
    data_out_range['field'] = data_out_range['field'] + "_OutOfRange"
    data_out_range.rename(columns={'outofrange': 'value'}, inplace=True)

    # Create datarange column
    data_range = data.copy()
    data_range.drop(['value', 'units', 'outofrange'], inplace=True, axis=1)
    data_range['field'] = data_range['field'] + "_Range"
    data_range.rename(columns={'range': 'value'}, inplace=True)

    # Append both of them
    data = data.append(data_out_range)
    data = data.append(data_range)

    # Extra variables
    date = soup.div.table.contents[4].contents[1].text
    report_date = soup.div.table.contents[8].contents[1].text
    data = data.append({'field': 'Collected:', 'value': date}, ignore_index=True)
    data = data.append({'field': 'Report Date:', 'value': report_date}, ignore_index=True)
    data = data.append(comments, ignore_index=True)
    data[cpgintegrate.SUBJECT_ID_FIELD_NAME] = study_id

    # Get rid of the variables we've turned into rows
    data.drop(['units', 'range', 'outofrange'], axis=1, inplace=True)

    # Get rid of any duplicates or nan fields
    data.drop_duplicates(inplace=True)
    data = data[data.value != "nan"]

    # Fix "fasting" measurements, since not having this mistake in the first place is apparently too complicated
    data['field'] = data['field'].str.replace("FASTING BLOOD GLUCOSE", "RANDOM BLOOD GLUCOSE")
    data['field'] = data['field'].str.replace("FASTING ", "")

    return ColumnInfoFrame(data.pivot(index=cpgintegrate.SUBJECT_ID_FIELD_NAME, columns="field", values="value"),
                           column_info=col_info)
