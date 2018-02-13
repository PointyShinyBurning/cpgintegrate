from airflow import DAG
from cpgintegrate.airflow.cpg_airflow_plugin import CPGDatasetToXCom, XComDatasetToCkan


def dataset_list_subdag(dag_id, start_date, connector_class, connection_id, ckan_connection_id, ckan_package_id, pool,
                        dataset_list):
    subdag = DAG(dag_id, start_date=start_date)
    with subdag as dag:
        for dataset in dataset_list:
            pull = CPGDatasetToXCom(task_id=dataset, connector_class=connector_class, connection_id=connection_id,
                                    dataset_args=[dataset], pool=pool)
            push = XComDatasetToCkan(task_id=dataset + '_ckan_push',
                                     ckan_connection_id=ckan_connection_id, ckan_package_id=ckan_package_id)
            pull >> push
    return subdag
