from abc import abstractmethod

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
import cpgintegrate
import requests
import pandas
from airflow.utils.state import State


class XComDatasetToCkan(BaseOperator):

    @apply_defaults
    def __init__(self, ckan_connection_id, ckan_package_id, check_freshness=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ckan_connection_id = ckan_connection_id
        self.ckan_package_id = ckan_package_id
        self.check_freshness = check_freshness

    def execute(self, context):
        conn = BaseHook.get_connection(self.ckan_connection_id)

        source_task_id = self.upstream_task_ids[0]

        push_frame = context['ti'].xcom_pull(source_task_id)

        if (push_frame[cpgintegrate.TIMESTAMP_FIELD_NAME].max() > context['dag_run'].start_date.timestamp())\
                or not self.check_freshness:

            existing_resource_list = requests.get(
                url=conn.host + '/api/3/action/package_show',
                headers={"Authorization": conn.get_password()},
                params={"id": self.ckan_package_id},
            ).json()['result']['resources']

            try:
                request_data = {"id": [res['id'] for res
                                       in existing_resource_list if res['name'] == source_task_id][0]}
                url_ending = '/api/3/action/resource_update'
            except IndexError:
                request_data = {"package_id": self.ckan_package_id, "name": source_task_id}
                url_ending = '/api/3/action/resource_create'
                self.log.info("Creating resource %s", self.ckan_package_id)

            res = requests.post(
                url=conn.host + url_ending,
                data=request_data,
                headers={"Authorization": conn.get_password()},
                files={"upload": (source_task_id+".csv", push_frame.to_csv())},
            )
            self.log.info("HTTP Status Code: %s", res.status_code)
            assert res.status_code == 200

            # Push metadata if exists'
            if hasattr(push_frame, 'get_json_column_info'):
                datadict_res = requests.post(
                    url=conn.host + '/api/3/action/datastore_create',
                    data='{"resource_id":"%s", "force":"true","fields":%s}' %
                         (res.json()['result']['id'], push_frame.get_json_column_info()),
                    headers={"Authorization": conn.get_password(), "Content-Type": "application/json"},
                )
                self.log.info("Data Dictionary Push Status Code: %s", datadict_res.status_code)
                assert datadict_res.status_code == 200
        else:
            self.log.info("Data unchanged in this run, not pushing")
            context['ti'].state = State.SKIPPED


class CPGCachingOperator(BaseOperator):

    @abstractmethod
    def _get_dataframe(self, context):
        pass

    def execute(self, context):
        out_frame = self._get_dataframe(context)
        xcom_pull = context['ti'].xcom_pull(self.task_id, include_prior_dates=True)
        old_frame = xcom_pull if xcom_pull is not None \
            else pandas.DataFrame({cpgintegrate.TIMESTAMP_FIELD_NAME: []})
        if cpgintegrate.TIMESTAMP_FIELD_NAME in old_frame.columns \
                and out_frame.drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1)\
                .equals(old_frame.drop(cpgintegrate.TIMESTAMP_FIELD_NAME, axis=1)):
            self.log.info("Old dataset still current, outputting that")
            return old_frame
        self.log.info("Dataset changed, outputting new one")
        return out_frame


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
        return self.connector_class(*self.connector_args,
                                    auth=(conn.login, conn.get_password()), **vars(conn), **self.connector_kwargs)

    def _get_dataframe(self, context):
        return self._get_connector().get_dataset(*self.dataset_args, **self.dataset_kwargs)


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
    def __init__(self, post_processor=None, filter_cols=None, drop_na_cols=True,
                 row_filter=lambda row: True, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        out_frame = self.post_processor(context['ti'].xcom_pull(self.upstream_task_ids[0])
                                        .filter(**self.column_filter)
                                        .loc[lambda df: df.apply(self.row_filter, axis=1)])
        if self.drop_na_cols:
            out_frame.dropna(axis=1, how='all', inplace=True)
        return out_frame


class AirflowCPGPlugin(AirflowPlugin):
    name = "cpg_plugin"
    operators = [CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan]
