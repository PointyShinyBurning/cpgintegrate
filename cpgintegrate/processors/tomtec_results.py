from cpgintegrate import ColumnInfoFrame
import cpgintegrate


def to_frame(file):
    """Turns txt export from Tomtec ARENA into dataframe

    :param file: A file-like object of the txt file
    :return: A DataFrame containing results
    """
    df = ColumnInfoFrame(index=[0])

    lines = file.read().decode('UTF-8').splitlines()
    dividers = iter([n for n, line in enumerate(lines) if line.startswith('====')] + [len(lines)])

    for start, end in zip(dividers, dividers):
        if lines[start - 1] != 'Curves':
            chunk = lines[start+1:end]
            try:
                prefix_divider = next(n for n, l in enumerate(chunk) if l.startswith('----'))
                prefix = "_".join(chunk[:prefix_divider])
            except StopIteration:
                prefix = None
            var_lines = [l.split(';') for l in chunk if ';' in l]
            for var_name, value, *units in var_lines:
                full_var_name = '_'.join([prefix, var_name]) if prefix else var_name
                df[full_var_name] = value
                if units:
                    df.add_column_info(full_var_name, {cpgintegrate.UNITS_ATTRIBUTE_NAME: units[0]})

    return df.set_index('Name')
