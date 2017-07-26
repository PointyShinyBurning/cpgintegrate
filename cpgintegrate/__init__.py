import pandas


def process_files(file_iterator, processor, cache=None):

    def get_frames():
        for file in file_iterator:
            yield processor(file)

    return pandas.concat((frame for frame in get_frames()))
