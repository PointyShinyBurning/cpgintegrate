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
        temp_dir = tempfile.TemporaryDirectory()
        os.chdir(temp_dir.name)

        temp_file = open("temp.pdf", "wb")
        temp_file.write(file.read())
        temp_file.close()

        os.system("pdftotext -raw temp.pdf")

        temp_txt = open("temp.txt", "r")
        file = temp_txt.read().splitlines()
        sheet = pandas.DataFrame({"SubjectID": [file[2].split(":")[1].split(" ")[0]]})
        for l in [re.split("[\[\]]", l) for l in file if l.startswith("Stiffness")]:
            sheet[l[0] + "(" + l[2].strip() + ")"] = l[1]

        data = pandas.DataFrame()
        for l in [re.split("[\[\]]", l) for l in file if not (l.startswith("Stiffness")) and ('[' in l) and 'kPa' in l]:
            data = data.append({"measNum": int(l[0][-3:-1]), "value (%s)" % l[2].strip(): l[1]}, ignore_index=True)

        temp_txt.close()
    else:
        excel_file = pandas.ExcelFile(file)

        meas = excel_file.parse()
        first_blank = meas[pandas.isnull(meas.ix[:, 0])].index[0]
        sheet = meas.iloc[first_blank:]

        if meas.columns[0].upper() == 'KPA':
            sheet.columns = ["val", "var"]
            data = pandas.DataFrame()
        else:
            sheet.columns = ["var", "val"]
            head = meas.columns[0]

            data = pandas.DataFrame({"value (kPa)": [head]})

        sheet.dropna(inplace=True, how="all")
        sheet['id'] = 1

        sheet.replace('(?i)kpa', '', inplace=True, regex=True)

        sheet = sheet.pivot("id", "var", "val")
        meas = meas.iloc[0:first_blank]
        meas.dropna(inplace=True, how="all")

        meas.columns = ['value (kPa)', 'measNum']
        data = data.append(meas)

        data['value (kPa)'].replace([' ', '(?i)kpa'], '', inplace=True, regex=True)

        data.measNum = range(1, len(data) + 1)

        sheet.rename(columns={"Median": "Stiffness Med (kPa)",
                              "SD": "Stiffness Std (kPa)",
                              "Stiffness Avg": "Stiffness Avg (kPa)",
                              "Stiffness Med": "Stiffness Med (kPa)",
                              "Stiffness Std": "Stiffness Std (kPa)",
                              },
                     inplace=True)

    return sheet.set_index("SubjectID")
