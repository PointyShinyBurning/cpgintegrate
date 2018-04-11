import pandas
import requests
from .connector import Connector


class CKAN(Connector):

    def __init__(self, auth: (str, str), host, **kwargs):
        super().__init__(**kwargs)
        self.auth = auth[1]
        self.host = host

    def _read_dataset(self, dataset, resource) -> pandas.DataFrame:
        resource_list = requests.get(
            url=self.host + '/api/3/action/package_show',
            headers={"Authorization": self.auth},
            params={"id": dataset},
        ).json()['result']['resources']

        resource_url = next(res['url'] for res in resource_list if res['name'] == resource or res['id'] == resource)

        return pandas.read_csv(requests.get(resource_url, headers={"Authorization": self.auth},
                                                      stream=True).raw)
