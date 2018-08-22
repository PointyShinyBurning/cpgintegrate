from cpgintegrate import ColumnInfoFrame
import cpgintegrate


def to_frame(file):
    """Turns txt export from Tomtec ARENA into dataframe

    :param file: A file-like object of the txt file
    :return: A DataFrame containing results
    """
    df = ColumnInfoFrame(index=[0])

    lines = file.read().decode('UTF-8').splitlines()
    dividers = [n for n, line in enumerate(lines) if line.startswith('====')] + [len(lines)]

    for start, end in zip(dividers[:-1], dividers[1:]):
        if lines[start - 1] != 'Curves':
            chunk = [line for line in lines[start+1:end-1] if line.strip() and not(line.startswith('----'))]
            prefix_lines = [l for l in chunk if not (';' in l)]
            prefix = "_".join(prefix_lines) if prefix_lines else None
            var_lines = [l.split(';') for l in chunk if ';' in l]
            for var_name, value, *units in var_lines:
                full_var_name = '_'.join([prefix, var_name]) if prefix else var_name
                df[full_var_name] = value
                if units:
                    df.add_column_info(full_var_name, {cpgintegrate.UNITS_ATTRIBUTE_NAME: units[0]})

    return df.set_index('Name')
