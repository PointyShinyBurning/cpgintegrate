from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
import cpgintegrate
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
import inspect


class XComDatasetToCkan(BaseOperator):

    @apply_defaults
    def __init__(self, ckan_connection_id, ckan_package_id, push_data_dictionary=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ckan_connection_id = ckan_connection_id
        self.ckan_package_id = ckan_package_id
        self.push_data_dictionary = push_data_dictionary

    def execute(self, context):

        conn = BaseHook.get_connection(self.ckan_connection_id)

        source_task_id = self.upstream_list[0].task_id

        push_frame = context['ti'].xcom_pull(source_task_id)
        old_frame = context['ti'].xcom_pull(self.task_id, include_prior_dates=True)

        existing_resource_list = requests.get(
            url=conn.host + '/api/3/action/package_show',
            headers={"Authorization": conn.get_password()},
            params={"id": self.ckan_package_id},
        ).json()['result']['resources']

        # Locate or create resource
        res_create = False
        try:
            request_data = {"id": next(res['id'] for res in existing_resource_list if res['name'] == source_task_id)}
            url_ending = '/api/3/action/resource_update'
        except StopIteration:
            request_data = {"package_id": self.ckan_package_id, "name": source_task_id}
            url_ending = '/api/3/action/resource_create'
            self.log.info("Creating resource %s", source_task_id)
            res_create = True

        if res_create or not (push_frame.equals(old_frame)):
            res = requests.post(
                url=conn.host + url_ending,
                data=request_data,
                headers={"Authorization": conn.get_password()},
                files={"upload": (source_task_id + ".csv", push_frame.to_csv(quoting=csv.QUOTE_NONNUMERIC))},
            )
            self.log.info("HTTP Status Code: %s", res.status_code)
            assert res.status_code == 200

            # Push metadata if exists'
            if hasattr(push_frame, 'get_json_column_info') and self.push_data_dictionary:

                self.log.info("Trying Data Dictionary Push")

                push_sess = requests.Session()
                push_sess.mount(conn.host, HTTPAdapter(
                    max_retries=Retry(status_forcelist=[409, 404], method_whitelist=['POST'],
                                      status=20, backoff_factor=0.1)))

                existing_dict_resp = push_sess.post(
                    url=conn.host + '/api/3/action/datastore_search',
                    data='{"resource_id":"%s"}' % res.json()['result']['id'],
                    headers={"Authorization": conn.get_password(), "Content-Type": "application/json"},
                )

                self.log.info("Data Dictionary Fetch Status Code: %s", existing_dict_resp.status_code)
                assert existing_dict_resp.status_code == 200

                existing_dict = existing_dict_resp.json()['result']['fields']

                for col in existing_dict:
                    if col['id'] in push_frame.columns and 'info' in col.keys():
                        push_frame.add_column_info(col['id'], col['info'])

                datadict_res = push_sess.post(
                    url=conn.host + '/api/3/action/datastore_create',
                    data='{"resource_id":"%s", "force":"true","fields":%s}' %
                         (res.json()['result']['id'], push_frame.get_json_column_info()),
                    headers={"Authorization": conn.get_password(), "Content-Type": "application/json"},
                )

                self.log.info("Data Dictionary Push Status Code: %s", datadict_res.status_code)
                assert datadict_res.status_code == 200

            return push_frame
        else:
            self.log.info("Frame unchanged, skipping push")


class CPGDatasetToXCom(BaseOperator):
    ui_color = '#7DF9FF'

    @apply_defaults
    def __init__(self, connector_class, connection_id, dataset_args=None, dataset_kwargs=None, connector_kwargs=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connector_class = connector_class
        self.connection_id = connection_id
        self.dataset_args = dataset_args or []
        self.dataset_kwargs = dataset_kwargs or {}
        self.connector_kwargs = connector_kwargs or {}

    def _get_connector(self):
        conn = BaseHook.get_connection(self.connection_id)
        return self.connector_class(auth=(conn.login, conn.get_password()),
                                    **{**vars(conn), **conn.extra_dejson, **self.connector_kwargs})

    def execute(self, context):
        return self._get_connector().get_dataset(*self.dataset_args, **self.dataset_kwargs)


class CPGProcessorToXCom(CPGDatasetToXCom):
    ui_color = '#E7FEFF'

    @apply_defaults
    def __init__(self, processor, iter_files_args=None, iter_files_kwargs=None, file_subject_id=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iter_files_args = iter_files_args or []
        self.iter_files_kwargs = iter_files_kwargs or {}
        self.processor = processor
        self.file_subject_id = file_subject_id

    def execute(self, context):
        connector_instance = self._get_connector()
        return (cpgintegrate
                .process_files(connector_instance.iter_files(*self.iter_files_args, **self.iter_files_kwargs),
                               self.processor)
                .drop([] if self.file_subject_id else cpgintegrate.FILE_SUBJECT_ID_FIELD_NAME, axis=1, errors='ignore'))


class XComDatasetProcess(BaseOperator):
    cols_always_present = [cpgintegrate.SOURCE_FIELD_NAME]

    @apply_defaults
    def __init__(self, post_processor=None, arg_map=None, task_id_kwargs=False, filter_cols=None, drop_na_cols=True,
                 row_filter=lambda row: True, keep_duplicates=None, *args, **kwargs):
        """
        Post processing on DataFrames from ancestor XCOMs

        :param post_processor: function to apply with all ancestor xcoms as args
        :param arg_map: dict that maps task_ids of upstream tasks to post_processor arguments
        :param task_id_kwargs: Submit all upstream xcoms to post_processor as task_id=dataframe kwargs?
        :param filter_cols: list of columns or str to translate as regex
        :param drop_na_cols: drop columns that are all na after post_processor?
        :param row_filter: row filter to apply to output
        :param keep_duplicates: 'last' or 'first' to keep only those duplicates indices
        """
        super().__init__(*args, **kwargs)
        self.arg_map = arg_map or {}
        self.task_id_kwargs = task_id_kwargs
        self.post_processor = post_processor or (lambda x: x)
        if type(filter_cols) == list:
            self.column_filter = {"items": filter_cols + self.cols_always_present}
        elif type(filter_cols) == str:
            self.column_filter = {"regex": filter_cols}
        else:
            self.column_filter = {"regex": ".*"}
        self.row_filter = row_filter
        self.drop_na_cols = drop_na_cols
        self.keep_duplicates = keep_duplicates

    def execute(self, context):
        arg_spec = inspect.getfullargspec(self.post_processor).args
        for task_id in self.upstream_task_ids:
            if task_id in arg_spec:
                self.arg_map.update({task_id: task_id})
        if self.task_id_kwargs:
            self.arg_map.update({task_id: task_id for task_id in self.upstream_task_ids})
        if len(self.upstream_list) == 1 and not self.arg_map:
            self.arg_map = {next(iter(self.upstream_task_ids)): arg_spec[0]}

        assert self.arg_map, "Must have an arg_map, task_id_kwargs or argument names that match task_ids" \
                             " for post_processors with multiple upstream tasks"
        out_frame = self.post_processor(**{self.arg_map[task_id]: context['ti'].xcom_pull(task_id)
                                           for task_id in self.upstream_task_ids})
        if self.drop_na_cols:
            out_frame.dropna(axis=1, how='all', inplace=True)
        if self.keep_duplicates:
            out_frame = out_frame.loc[~out_frame.index.duplicated(keep=self.keep_duplicates)]
        return out_frame.filter(**self.column_filter).loc[lambda df: df.apply(self.row_filter, axis=1)]


class AirflowCPGPlugin(AirflowPlugin):
    name = "cpg_plugin"
    operators = [CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan]
