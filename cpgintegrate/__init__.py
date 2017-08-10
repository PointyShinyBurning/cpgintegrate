import pandas
import typing


def process_files(file_iterator: typing.Iterator[typing.IO], processor: typing.Callable) -> pandas.DataFrame:

    def get_frames():
        for file in file_iterator:
            source = getattr(file, 'name', None)
            subject_id = getattr(file, 'cpgintegrate_subject_id', None)
            try:
                df = processor(file)
            except Exception as e:
                raise ProcessingException({"Source": source, 'SubjectID': subject_id}) from e
            yield (df
                   .assign(Source=getattr(file, 'name', None),
                           SubjectID=getattr(file, 'cpgintegrate_subject_id', None),
                           FileSubjectID=df.index if df.index.name else None))

    return pandas.DataFrame(pandas.concat((frame for frame in get_frames()))).set_index("SubjectID")


class ProcessingException(Exception):
    """cpgintegrate processing error"""
