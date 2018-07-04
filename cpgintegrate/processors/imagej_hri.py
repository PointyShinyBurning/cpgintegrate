import pandas


def to_frame(file):
    """
    ImageJ analysis of Hepatorenal Index
    :param file: file-like of results csv
    :return: pandas.DataFrame of results
    """

    dat = pandas.read_csv(file, sep="\t").dropna(how="all")
    sheet = pandas.melt(dat.iloc[0:2], id_vars="Label")

    sheet = sheet[sheet.variable != " "].dropna()
    sheet.variable = [s.split(":")[1] + '_' for s in sheet.Label] + sheet.variable
    sheet.Label = [s.split(".")[0] for s in sheet.Label]
    assert len(sheet.Label.drop_duplicates()) == 1
    sheet.rename(columns={"Label": "SubjectID"}, inplace=True)
    sheet = sheet.pivot(index="SubjectID", columns="variable", values="value")
    sheet['SubjectID'] = sheet.index
    sheet.index.name = 'SubjectID'

    try:
        sheet['HRI'] = sheet.Liver_Mean / sheet.Kidney_Mean
    except AttributeError:
        sheet['HRI'] = sheet['Kidney-1_Mean'] / sheet.Kidney_Mean

    sheet.HRI = sheet.HRI.astype("float").round(2)

    return sheet
