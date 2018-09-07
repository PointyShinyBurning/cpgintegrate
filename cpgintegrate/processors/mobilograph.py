import pandas
import io


def to_frame(file):
    """Turns txt export from IEM MOBIL-O-GRAPH into dataframe

    :param file: A file-like object of the xls
    :return: A DataFrame containing results
    """
    file_bytes = file.read()

    top_vars = (pandas.read_excel(io.BytesIO(file_bytes), nrows=8, header=None)
                .dropna(axis=1, how='all').set_index(0).T.assign(join_key=0)
                )

    measurements = pandas.read_excel(io.BytesIO(file_bytes), skiprows=13).assign(join_key=0).set_index('join_key')

    return top_vars.join(measurements, on='join_key').set_index('ID').drop('join_key', axis=1)
