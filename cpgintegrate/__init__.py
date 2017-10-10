import pandas
import typing
import inspect
import hashlib
import time
from .column_info_frame import ColumnInfoFrame

SUBJECT_ID_ATTR = 'cpgintegrate_subject_id'
CACHE_KEY_ATTR = 'cpgintegrate_hash_key'

TIMESTAMP_FIELD_NAME = 'timestamp'
SOURCE_FIELD_NAME = 'Source'
SUBJECT_ID_FIELD_NAME = 'SubjectID'
FILE_SUBJECT_ID_FIELD_NAME = 'FileSubjectID'

UNITS_ATTRIBUTE_NAME = 'label'
DESCRIPTION_ATTRIBUTE_NAME = 'notes'


def process_files(file_iterator: typing.Iterator[typing.IO], processor) -> pandas.DataFrame:

    processing_func = processor if callable(processor) else processor.to_frame
    processing_func_hash = hashlib.sha256(inspect.getsource(processing_func).encode()).digest()

    def get_frames():
        for file in file_iterator:
            source = getattr(file, 'name', None)
            subject_id = getattr(file, SUBJECT_ID_ATTR, None)
            try:
                df = processing_func(file)
            except Exception as e:
                raise ProcessingException({SOURCE_FIELD_NAME: source, SUBJECT_ID_FIELD_NAME: subject_id}) from e
            finally:
                file.close()
            yield (df
                   .assign(**{SUBJECT_ID_FIELD_NAME: subject_id,
                              FILE_SUBJECT_ID_FIELD_NAME: (df.index if df.index.name else None),
                              SOURCE_FIELD_NAME: source, TIMESTAMP_FIELD_NAME: time.time()}))

    return pandas.concat((frame for frame in get_frames())).set_index(SUBJECT_ID_FIELD_NAME)


class ProcessingException(Exception):
    """cpgintegrate processing error"""
