from abc import abstractmethod

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
import cpgintegrate
import logging
import requests
import pandas


class XComDatasetToCkan(BaseOperator):

    @apply_defaults
    def __init__(self, source_task_id, ckan_connection_id, ckan_package_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_task_id = source_task_id
        self.ckan_connection_id = ckan_connection_id
        self.ckan_package_id = ckan_package_id

    def execute(self, context):
        conn = BaseHook.get_connection(self.connection_id)

        push_frame = context['ti'].xcom_pull(self.source_task_id) or pandas.DataFrame()
        existing_resource_list = requests.get(
            url=conn.host + '/api/3/action/package_show',
            headers={"Authorization": conn.get_password()},
            params={"id": self.ckan_package_id},
        ).json()['result']['resources']

        try:
            request_data = {"id": [res['id'] for res in existing_resource_list if res['name'] == self.source_task_id][0]}
            url_ending = '/api/3/action/resource_update'
        except IndexError:
            request_data = {"package_id": self.ckan_package_id, "name": self.source_task_id}
            url_ending = '/api/3/action/resource_create'
            logging.info("Creating resource %s", self.ckan_package_id)

        res = requests.post(
            url=conn.host + url_ending,
            data=request_data,
            headers={"Authorization": conn.get_password()},
            files={"upload": push_frame.to_csv()},
        )
        logging.info("HTTP Status Code: %s", res.status_code)
        assert res.status_code == 200

        # Push metadata if exists'
        if hasattr(push_frame, 'get_json_column_info'):
            datadict_res = requests.post(
                url=conn.host + '/api/3/action/datastore_create',
                data='{"resource_id":"%s", "force":"true","fields":%s}' %
                     (res.json()['result']['id'], push_frame.get_json_column_info()),
                headers={"Authorization": conn.get_password(), "Content-Type": "application/json"},
            )
            logging.info("Data Dictionary Push Status Code: %s", datadict_res.status_code)
            assert datadict_res.status_code == 200


class CPGCachingOperator(BaseOperator):

    @abstractmethod
    def _get_dataframe(self, context):
        pass

    def execute(self, context):
        out_frame = self._get_dataframe(context)
        old_frame = context['ti'].xcom_pull(self.task_id, include_prior_dates=True) \
                    or pandas.DataFrame({cpgintegrate.TIMESTAMP_FIELD_NAME: []})
        if not (out_frame
                        .drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1)
                        .equals(old_frame.drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1))):
            return out_frame
        return old_frame


class CPGDatasetToXCom(CPGCachingOperator):
    ui_color = '#7DF9FF'

    @apply_defaults
    def __init__(self, connector_class, connection_id, connector_args, connector_kwargs=None,
                 dataset_args=None, dataset_kwargs=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connector_class = connector_class
        self.connection_id = connection_id
        self.connector_args = connector_args
        self.connector_kwargs = connector_kwargs or {}
        self.dataset_args = dataset_args or []
        self.dataset_kwargs = dataset_kwargs or {}

    def _get_connector(self):
        conn = BaseHook.get_connection(self.connection_id)
        return self.connector_class(conn.host, *self.connector_args,
                                    auth=(conn.login, conn.password), **self.connector_kwargs)

    def _get_dataframe(self, context):
        return self._get_connector().get_dataset(*self.dataset_args, **self.dataset_kwargs)

    def execute(self, context):
        out_frame = self._get_dataframe(context)
        xcom_pull = context['ti'].xcom_pull(self.task_id, include_prior_dates=True)
        old_frame = xcom_pull if xcom_pull is not None \
            else pandas.DataFrame({cpgintegrate.TIMESTAMP_FIELD_NAME: []})
        if not out_frame.drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1)\
                .equals(old_frame.drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1)):
            return out_frame
        return old_frame


class CPGProcessorToXCom(CPGDatasetToXCom):
    ui_color = '#E7FEFF'

    @apply_defaults
    def __init__(self, processor, iter_files_args=None, iter_files_kwargs=None,
                 processor_args=None, processor_kwargs=None, file_subject_id=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iter_files_args = iter_files_args or []
        self.iter_files_kwargs = iter_files_kwargs or {}
        self.processor = processor
        self.processor_args = processor_args or []
        self.processor_kwargs = processor_kwargs or {}
        self.file_subject_id = file_subject_id

    def _get_dataframe(self, context):
        connector_instance = self._get_connector()
        processor_instance = self.processor(*self.processor_args, **self.processor_kwargs).to_frame \
            if isinstance(self.processor, type) else self.processor
        return (cpgintegrate
                .process_files(connector_instance.iter_files(*self.iter_files_args, **self.iter_files_kwargs),
                               processor_instance)
                .drop([] if self.file_subject_id else ["FileSubjectID"], axis=1))


class XComDatasetProcess(CPGCachingOperator):
    cols_always_present = [cpgintegrate.TIMESTAMP_FIELD_NAME, cpgintegrate.SOURCE_FIELD_NAME]

    @apply_defaults
    def __init__(self, source_task_id, post_processor=None, filter_cols=None, drop_na_cols=True,
               row_filter=lambda row: True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_task_id = source_task_id
        self.post_processor = post_processor or (lambda x: x)
        if type(filter_cols) == list:
            self.column_filter = {"items": filter_cols+self.cols_always_present}
        elif type(filter_cols) == str:
            self.column_filter = {"regex": str}
        else:
            self.column_filter = {"regex": ".*"}
        self.row_filter = row_filter
        self.drop_na_cols = drop_na_cols

    def _get_dataframe(self, context):
        out_frame = self.post_processor(context['ti'].xcom_pull(self.source_task_id)
                                        .filter(**self.column_filter)
                                        .loc[lambda df: df.apply(self.row_filter, axis=1)])
        if self.drop_na_cols:
            out_frame.dropna(axis=1, how='all', inplace=True)
        return out_frame


class AirflowCPGPlugin(AirflowPlugin):
    name = "cpg_plugin"
    operators = [CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan]
