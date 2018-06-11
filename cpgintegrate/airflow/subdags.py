from airflow.operators.cpg_plugin import CPGDatasetToXCom, XComDatasetToCkan
from airflow.models import DAG


def dataset_list_subdag(dag_id, start_date, connector_class, connection_id, ckan_connection_id, ckan_package_id, pool,
                        dataset_list, schema=None, connector_kwargs={}):
    subdag = DAG(dag_id, start_date=start_date)
    with subdag as dag:
        for dataset in dataset_list:

            pull = CPGDatasetToXCom(task_id=dataset, connector_class=connector_class, connection_id=connection_id,
                                    connector_kwargs=connector_kwargs,
                                    dataset_args=[dataset], pool=pool)
            push = XComDatasetToCkan(task_id=dataset + '_ckan_push',
                                     ckan_connection_id=ckan_connection_id, ckan_package_id=ckan_package_id)
            pull >> push

            # Puts them a row because airflow 1.9.0 doesn't respect pools in subDags
            try:
                last >> pull
            except NameError:
                pass
            last = pull
    return subdag