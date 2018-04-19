from cpgintegrate.column_info_frame import ColumnInfoFrame
import cpgintegrate
from collections import OrderedDict


def to_frame(file):
    data = OrderedDict()
    col_info = {}

    pairs = [(line[3:7], line[7:]) for line
             in file.read().decode().splitlines()]

    data['SubjectID'] = [b for a, b in pairs if a == '3102']
    if 'SABRE' in data['SubjectID']:
        data['SubjectID'] = [b for a, b in pairs if a == '3101']

    i = iter(pairs)

    try:
        while True:
            cur = next(i)
            if cur[0] == '8410':
                head = cur[1]
                val = next(i)[1]
                units = next(i)[1]
                data[head] = val
                if units != '---':
                    col_info[head] = {cpgintegrate.UNITS_ATTRIBUTE_NAME: units}
    except StopIteration:
        pass

    return ColumnInfoFrame(data, column_info=col_info).set_index('SubjectID')