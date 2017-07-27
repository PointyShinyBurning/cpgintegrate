import pandas
import traceback
import typing


def process_files(file_iterator: typing.Iterator[typing.IO], processor: typing.Callable) -> pandas.DataFrame:

    def get_frames():
        for file in file_iterator:
            try:
                df = processor(file)
            except Exception:
                df = pandas.DataFrame({"error": [traceback.format_exc()]})

            yield (df
                   .assign(Source=getattr(file, 'name', None),
                           SubjectID=getattr(file, 'cpgintegrate_subject_id', None),
                           FileSubjectID=df.index if df.index.name else None))

    return pandas.DataFrame(pandas.concat((frame for frame in get_frames()))).set_index("SubjectID")
