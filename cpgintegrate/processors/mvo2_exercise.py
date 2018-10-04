import pandas
import math
import datetime
from scipy import stats
import numpy as np
from . import units_in_brackets_to_col_info


def to_frame(file, line_data=False):
    # Eats some bytes and spits out exercise test results
    # print("Processing:" + fileName + "(" + subjectID + ")")

    sheet = pandas.DataFrame()
    excel_file = pandas.ExcelFile(file)
    for r in ["A:B", "D:E", "G:H"]:
        sheet = sheet.append(excel_file.parse(0,
                                              usecols=r,
                                              index_col=None,
                                              header=None),
                             ignore_index=True
                             )
    sheet = sheet[sheet[0] == sheet[0]]
    sheet.loc[sheet[0].str.endswith(":"), 0] = sheet.loc[sheet[0].str.endswith(":"), 0].str[:-1]
    sheet['i'] = 1
    sheet = sheet.drop_duplicates(0)
    sheet = sheet.pivot('i', 0, 1)

    data = excel_file.parse(0, usecols="J:DX", index_col=None)
    data = data.drop(data.index[[0, 1]])
    data = data[data['t'] == data['t']]

    # Get seconds out of t
    data['sec'] = [datetime.timedelta(seconds=int(s), minutes=int(m), hours=int(h)).total_seconds()
                   for h, m, s in data['t'].str.split(":")
                   ]
    data.index = data['sec']

    # Max 60 second rolling averages in phase 3/4
    avrgs = ['VO2', 'VCO2', 'VE']
    phases = [1, 3, 4]
    for var in avrgs:
        data[var].replace(0, np.NaN, inplace=True)
        data[var + '60secavg'] = [data[max((x['sec'] - 60), 0): x['sec']][var].mean()
                                  for i, x in data.iterrows()
                                  ]
        for phase in phases:
            sheet[var + 'avrg Phase ' + str(phase)] = data[data['Phase'] == phase][var + '60secavg'].max()
        # 3-4 Transition
        try:
            phase_3_4_midpoint = (data[data['Phase'] == 3].index[-1] + data[data['Phase'] == 4].index[0]) / 2
            sheet[var + 'avrg Phase 3/4 transition'] = \
                data[phase_3_4_midpoint - 30:phase_3_4_midpoint + 30][var + '60secavg'].max()
        except IndexError:
            pass

    if line_data:
        return data

    # ratios
    sheet['Min VE60secavg/VO260secavg(l/min)'] = (data.VE60secavg / data.VO260secavg).min()
    sheet['Min VE60secavg/VCO260secavg(l/min)'] = (data.VE60secavg / data.VCO260secavg).min()
    slope, _, r, _, _ = stats.linregress(data.VE60secavg, data.VCO260secavg)
    sheet['VE60secavg/VCO260secavg slope'] = slope
    sheet['VE60secavg/VCO260secavg r^2'] = np.square(r)

    # Max and min heartrates
    phases = [1, 3, 4]
    hrs = data[(30 <= data['HR']) & (data['HR'] <= 300)]
    for phase in phases:
        sheet['Min HR Phase ' + str(phase)] = hrs[hrs.Phase == phase].HR.min()
        sheet['Peak HR Phase ' + str(phase)] = hrs[hrs.Phase == phase].HR.max()

    sheet['Peak HR'] = hrs.HR.max()
    sheet['Min HR'] = hrs.HR.min()

    # OUES
    phase3 = data[data['Phase'] == 3].copy()
    if len(phase3.index):
        phase3['Log VE'] = [math.log10(x) for x in phase3['VE']]
        phase3['L/min'] = [x / 1000 for x in phase3['VO2']]
        sheet['OUES'], _, _, _, _ = stats.linregress(phase3['Log VE'], phase3['L/min'])

    # Append max variables
    for phase in [1, 2, 3]:
        sheet['RER Phase %s' % phase] = data[data['Phase'] == phase]['R'].max()

    sheet['VE'] = data['VE'].max()

    # Drop step, spiro data (victims not equipped with it)
    sheet.drop(['HR max (bpm)', 'User 1', 'User 2', 'User 3',
                'UN (g/day)', 'MVV (l/min)', 'FEV1 (l)', 'FVC (l)', 'PaCO2 @ peak (mmHg)',
                'PaCO2 @ rest (mmHg)', 'PaO2 @ peak (mmHg)', 'PaO2 @ rest (mmHg)', 'First name'], 1, inplace=True)

    return units_in_brackets_to_col_info(sheet.set_index('ID code'))
