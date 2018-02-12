from airflow import DAG
from airflow.operators.cpg_plugin import CPGDatasetToXCom, XComDatasetToCkan


def dataset_list_to_ckan(target_dag: DAG, connector_class, connection_id,
                         ckan_connection_id, ckan_package_id, pool, dataset_list):
    with target_dag as dag:
        for dataset in dataset_list:
            pull = CPGDatasetToXCom(connector_class=connector_class, connection_id=connection_id,
                                    dataset_args=[dataset], pool=pool)
            push = XComDatasetToCkan(ckan_connection_id=ckan_connection_id, ckan_package_id=ckan_package_id)
            pull >> push
