import pandas
import os
import re
import tempfile


def to_frame(file):
    """Turns pdf or excel liver elastography analysis from Philips EPIQ 7 into a DataFrame
    Needs pdftotext from xpdf (http://www.foolabs.com/xpdf/home.html) to be somewhere in the PATH

    :param file: A file-like object that has a .name
    :return: A DataFrame containing the stiffness stats
    """

    if file.name.endswith(".pdf"):
        with tempfile.TemporaryDirectory() as temp_dir:

            temp_file = open(os.path.join(temp_dir, "temp.pdf"), "wb")
            temp_file.write(file.read())
            temp_file.close()

            os.system("pdftotext -raw -enc UTF-8 %s" % temp_file.name)

            temp_txt = open(os.path.join(temp_dir, "temp.txt"), "r")
            file = temp_txt.read().splitlines()
            sheet = pandas.DataFrame({"SubjectID": [file[2].split(":")[1].split(" ")[0]]}).set_index("SubjectID")
            for l in [re.split("[\[\]]", l) for l in file if l.startswith("Stiffness")]:
                sheet[l[0] + "(" + l[2].strip() + ")"] = l[1]

            temp_txt.close()

            # Correct m/s to kPa
            if sheet.columns[0].endswith("(m / s)"):
                sheet = (sheet.apply(pandas.to_numeric).transform(lambda x: x**2 * 3)
                         .rename(columns=lambda col: col.replace("m / s", "kPa")))
    else:
        excel_file = pandas.ExcelFile(file)

        meas = excel_file.parse()
        first_blank = meas[pandas.isnull(meas.ix[:, 0])].index[0]
        sheet = meas.iloc[first_blank:]

        if meas.columns[0].upper() == 'KPA':
            sheet.columns = ["val", "var"]
        else:
            sheet.columns = ["var", "val"]

        sheet.dropna(inplace=True, how="all")
        sheet.loc[:, 'id'] = 1

        sheet.replace('(?i)kpa', '', inplace=True, regex=True)

        sheet = sheet.pivot("id", "var", "val")
        sheet.index.name = None
        meas = meas.iloc[0:first_blank]
        meas.dropna(inplace=True, how="all")

        meas.columns = ['value (kPa)', 'measNum']

        sheet.rename(columns={"Median": "Stiffness Med (kPa)",
                              "SD": "Stiffness Std (kPa)",
                              "Avg": "Stiffness Avg (kPa)",
                              "Stiffness Avg": "Stiffness Avg (kPa)",
                              "Stiffness Med": "Stiffness Med (kPa)",
                              "Stiffness Std": "Stiffness Std (kPa)",
                              },
                     inplace=True)

    return sheet
