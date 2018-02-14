import pandas
import numpy, itertools, io


def to_frame(file):
    # Blood pressure results from file-ish object

    file_as_string = file.read().decode('utf-8')

    if file_as_string.startswith("Measurement"):
        # Format from new software in June 2017, harmonise to old format even though it's objectively worse
        print("New format: %s" % file.name)
        sheet = pandas.DataFrame(index=[0])

        data = pandas.read_csv(io.StringIO(file_as_string))
        data.columns = (col.strip() for col in data.columns)

        # Extra variable because new version doesn't have seconds in timestamp
        data['Meas_num'] = data.groupby("Measurement").cumcount()

        data_measures = data.melt(["Meas_num", "Measurement"], value_vars="Data")
        data_other = data.melt("Meas_num", ["Data & Alerts", "Date"]).drop_duplicates(["Meas_num", "variable"])
        data_measures['variable'] = data_measures['Measurement']

        data = pandas.concat([data_measures, data_other]).pivot("Meas_num", "variable", "value")

        data['Date'], data['Time'] = zip(*data['Date'].str.split())

        data.rename(columns={"Pulse": "Pulse (Pulses/min)",
                             "Systolic": "SYS (mmHg)",
                             "Diastolic": "DIA (mmHg)"}, inplace=True)

        data['Irregular Heartbeat (y or n)'], data['Excessive Movement (y or n)'] = zip(*[
            ("y" if alert == " Irregular Heartbeat" else "n",
             "y" if alert == " Excessive Movement" else "n") for alert in data["Data & Alerts"]
        ])

        data.drop("Data & Alerts", axis=1, inplace=True)
    else:
        sheet = pandas.DataFrame({"SubjectID": [file_as_string.splitlines()[0].split(",")[0]]})

        data = pandas.read_csv(io.StringIO(file_as_string), skiprows=2)

    avgs = ['SYS (mmHg)', 'DIA (mmHg)']

    # Mean of 2nd and 3rd if difference <= 10, otherwise mean of lowest difference pair
    for avg in avgs:
        if len(data[avg]) < 3:
            sheet[avg] = data[avg].iloc[-1]
        else:
            seq = data[avg][1:]
            if (seq[1] - seq[2]) < 10:
                sheet[avg] = seq[1:3].mean()
            else:
                pairs = [(numpy.mean([i, i1]), abs(i-i1)) for i, i1 in itertools.combinations(seq, 2)]
                sheet[avg] = min(pairs, key=lambda t: t[1])[0]

    sheet['Pulse (Pulses/min)'] = data['Pulse (Pulses/min)'][1:].mean()
    sheet['Irregular Hearbeats'] = len(data[data['Irregular Heartbeat (y or n)'] == 'y'].index)
    sheet['Excessive Movements'] = len(data[data['Excessive Movement (y or n)'] == 'y'].index)

    return sheet
