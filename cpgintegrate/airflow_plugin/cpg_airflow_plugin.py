from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
import cpgintegrate
import os
import logging


class CPGDatasetToCsv(BaseOperator):
    @apply_defaults
    def __init__(self, connector_class, connection_id, connector_args, csv_dir,
                 connector_kwargs=None, dataset_args=None, dataset_kwargs=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connector_class = connector_class
        self.connection_id = connection_id
        self.connector_args = connector_args
        self.connector_kwargs = connector_kwargs or {}
        self.dataset_args = dataset_args or []
        self.dataset_kwargs = dataset_kwargs or {}
        self.csv_path = os.path.join(csv_dir, self.task_id + ".csv")

    def _get_connector(self):
        conn = BaseHook.get_conn(self.connection_id)
        return self.connector_class(conn.host, *self.connector_args,
                                    auth=(conn.login, conn.password), **self.connector_kwargs)

    def _get_dataframe(self):
        return self._get_connector().get_dataset(*self.dataset_args, **self.dataset_kwargs)

    def execute(self, context):
        out_frame = self._get_dataframe()
        if not (context['ti'].xcom_pull(self.task_id, include_prior_dates=True).equals(out_frame)):
            logging.info("Dataset changed from last run, outputting csv")
            out_frame.to_csv(self.csv_path)
        else:
            logging.info("Dataset same as last run, leaving csv alone")
        return out_frame


class CPGProcessorToCsv(CPGDatasetToCsv):
    @apply_defaults
    def __init__(self, processor, iter_files_args=None, iter_files_kwargs=None,
                 processor_args=None, processor_kwargs=None, filter_cols=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iter_files_args = iter_files_args or []
        self.iter_files_kwargs = iter_files_kwargs or {}
        self.processor = processor
        self.processor_args = processor_args or []
        self.processor_kwargs = processor_kwargs or {}
        self.filter_cols = {"items": filter_cols} or {"regex": ".*"}

    def _get_dataframe(self):
        connector_instance = self._get_connector()
        processor_instance = self.processor(*self.processor_args, **self.processor_kwargs).to_frame \
            if isinstance(self.processor, type) else self.processor
        return (cpgintegrate
                .process_files(connector_instance.iter_files(*self.iter_files_args, **self.iter_files_kwargs),
                               processor_instance)
                .filter(axis='columns', **self.filter_cols))


class AirflowCPGPlugin(AirflowPlugin):
    name = "cpg_plugin"
    operators = [CPGDatasetToCsv, CPGProcessorToCsv]
