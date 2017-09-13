from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
import cpgintegrate
import os
import logging
from walrus import Walrus

class CPGDatasetToCsv(BaseOperator):
    ui_color = '#7DF9FF'
    cols_always_present = ['Source']

    @apply_defaults
    def __init__(self, connector_class, connection_id, connector_args, csv_dir,
                 connector_kwargs=None, dataset_args=None, dataset_kwargs=None, post_processor=None, filter_cols=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connector_class = connector_class
        self.connection_id = connection_id
        self.connector_args = connector_args
        self.connector_kwargs = connector_kwargs or {}
        self.dataset_args = dataset_args or []
        self.dataset_kwargs = dataset_kwargs or {}
        self.csv_path = os.path.join(csv_dir, self.task_id + ".csv")
        self.post_processor = post_processor or (lambda x: x)
        if type(filter_cols) == list:
            self.column_filter = {"items": filter_cols+self.cols_always_present}
        elif type(filter_cols) == str:
            self.column_filter = {"regex": str}
        else:
            self.column_filter = {"regex": ".*"}

    def _get_connector(self):
        conn = BaseHook.get_connection(self.connection_id)
        return self.connector_class(conn.host, *self.connector_args,
                                    auth=(conn.login, conn.password), **self.connector_kwargs)

    def _get_dataframe(self):
        return self._get_connector().get_dataset(*self.dataset_args, **self.dataset_kwargs)

    def execute(self, context):
        out_frame = self.post_processor(self._get_dataframe().filter(**self.column_filter))
        old_frame = context['ti'].xcom_pull(self.task_id, include_prior_dates=True)
        if not(out_frame.equals(old_frame)) or not(os.path.exists(self.csv_path)):
            logging.info("Dataset changed from last run, outputting csv")
            out_frame.to_csv(self.csv_path)
        else:
            logging.info("Dataset same as last run, leaving csv alone")
        return out_frame


class CPGProcessorToCsv(CPGDatasetToCsv):
    ui_color = '#E7FEFF'

    @apply_defaults
    def __init__(self, processor, iter_files_args=None, iter_files_kwargs=None, cache_name=None,
                 processor_args=None, processor_kwargs=None, file_subject_id=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iter_files_args = iter_files_args or []
        self.iter_files_kwargs = iter_files_kwargs or {}
        self.processor = processor
        self.processor_args = processor_args or []
        self.processor_kwargs = processor_kwargs or {}
        self.file_subject_id = file_subject_id
        self.cache_name = cache_name

    def _get_dataframe(self):
        connector_instance = self._get_connector()
        processor_instance = self.processor(*self.processor_args, **self.processor_kwargs).to_frame \
            if isinstance(self.processor, type) else self.processor
        return (cpgintegrate
                .process_files(connector_instance.iter_files(*self.iter_files_args, **self.iter_files_kwargs),
                               processor_instance, cache=Walrus().Hash(self.cache_name) if self.cache_name else None)
                .drop([] if self.file_subject_id else ["FileSubjectID"], axis=1))


class AirflowCPGPlugin(AirflowPlugin):
    name = "cpg_plugin"
    operators = [CPGDatasetToCsv, CPGProcessorToCsv]
