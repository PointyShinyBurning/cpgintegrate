import pandas
import typing
from .column_info_frame import ColumnInfoFrame
import itertools

SUBJECT_ID_ATTR = 'cpgintegrate_subject_id'
CACHE_KEY_ATTR = 'cpgintegrate_hash_key'

SOURCE_FIELD_NAME = 'Source'
SUBJECT_ID_FIELD_NAME = 'SubjectID'
FILE_SUBJECT_ID_FIELD_NAME = 'FileSubjectID'

UNITS_ATTRIBUTE_NAME = 'label'
DESCRIPTION_ATTRIBUTE_NAME = 'notes'


def process_files(file_iterator: typing.Iterator[typing.IO], processor, limit=None) -> pandas.DataFrame:

    processing_func = processor if callable(processor) else processor.to_frame

    def get_frames():
        for file in itertools.islice(file_iterator, limit):
            source = getattr(file, 'name', None)
            subject_id = getattr(file, SUBJECT_ID_ATTR, None)
            try:
                df = processing_func(file)
            except Exception as e:
                raise ProcessingException({SOURCE_FIELD_NAME: source, SUBJECT_ID_FIELD_NAME: subject_id}) from e
            finally:
                file.close()
            yield (df.assign(
                    **{SUBJECT_ID_FIELD_NAME: df.index
                        if df.index.name == SUBJECT_ID_FIELD_NAME and subject_id is None else subject_id,
                        FILE_SUBJECT_ID_FIELD_NAME: (df.index if df.index.name else None),
                        SOURCE_FIELD_NAME: source, }))

    def ordering_sequence(sequences: [[str]]) -> [str]:
        """Item pairs in output sequence respect order of at least one input sequence containing them (if it exists)"""
        output = []
        first_pass = True
        non_empty_sequence = False
        while non_empty_sequence or first_pass:
            first_pass = False
            non_empty_sequence = False
            for sequence in sequences:
                try:
                    item = sequence.pop(0)
                    non_empty_sequence = True
                    if item not in output:
                        output.append(item)
                except IndexError:
                    pass
        return output

    frames = [frame for frame in get_frames()]
    column_order = ordering_sequence([list(frame.columns) for frame in frames])
    return pandas.concat(frames).loc[:, column_order].set_index(SUBJECT_ID_FIELD_NAME)


class ProcessingException(Exception):
    """cpgintegrate processing error"""
