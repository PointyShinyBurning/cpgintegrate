import pandas
import typing
import inspect
import hashlib
import logging

SUBJECT_ID_ATTR = 'cpgintegrate_subject_id'
CACHE_KEY_ATTR = 'cpgintegrate_hash_key'


def process_files(file_iterator: typing.Iterator[typing.IO], processor, cache: typing.Dict=None) -> pandas.DataFrame:

    processing_func = processor if callable(processor) else processor.to_frame
    processing_func_hash = hashlib.sha256(inspect.getsource(processing_func).encode()).digest()

    def get_frames():
        for file in file_iterator:
            source = getattr(file, 'name', None)
            subject_id = getattr(file, SUBJECT_ID_ATTR, None)
            cache_key = (source, processing_func_hash, getattr(file, CACHE_KEY_ATTR, None))
            try:
                try:
                    assert source and cache is not None
                    df = pandas.read_msgpack(cache[cache_key])
                    logging.debug("Cache hit for %s", source)
                except (KeyError, AssertionError):
                    logging.debug("Calculating output for %s", source)
                    df = processing_func(file)
                    if source and cache is not None:
                        cache[cache_key] = df.to_msgpack(compress='zlib')
                file.close()
            except Exception as e:
                raise ProcessingException({"Source": source, 'SubjectID': subject_id}) from e
            yield (df
                   .assign(Source=source, SubjectID=subject_id, FileSubjectID=df.index if df.index.name else None))

    return pandas.DataFrame(pandas.concat((frame for frame in get_frames()))).set_index("SubjectID")


class ProcessingException(Exception):
    """cpgintegrate processing error"""
