import pandas
import requests
from .connector import Connector
import cpgintegrate


class CKAN(Connector):

    def __init__(self, auth: (str, str), host, **kwargs):
        super().__init__(**kwargs)
        self.auth = auth[1]
        self.host = host

    def _read_dataset(self, dataset, resource, index_col=cpgintegrate.SUBJECT_ID_FIELD_NAME) -> pandas.DataFrame:
        resource_list = requests.get(
            url=self.host + '/api/3/action/package_show',
            headers={"Authorization": self.auth},
            params={"id": dataset},
        ).json()['result']['resources']

        resource_url = next(res['url'] for res in resource_list if res['name'] == resource or res['id'] == resource)

        return (pandas
                .read_csv(requests.get(resource_url, headers={"Authorization": self.auth}, stream=True).raw)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: resource_url})
                .pipe(lambda df: df.set_index(index_col) if index_col and index_col in df.columns else df))
