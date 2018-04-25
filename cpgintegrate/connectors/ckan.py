import typing

import pandas
import requests
from .connector import FileDownloadingConnector
import cpgintegrate
from pathlib import Path
import os
from typing import Union, Tuple


class CKAN(FileDownloadingConnector):

    def __init__(self, host="https://localhost/ckan", auth: Union[str, Tuple[str, str], None]=None, **kwargs):

        """
        CKAN API Key will be read from ~/ckan_api_key file if auth not given

        :param host: Base url of ckan instance
        :param auth: CKAN API Key from users ckan home page, as string or second element of tuple
        """
        super().__init__(**kwargs)
        if type(auth) == str:
            self.auth = auth
        elif type(auth) == tuple:
            self.auth = auth[1]
        else:
            self.auth = open(os.path.join(Path.home(), '.ckan_api_key')).read()
        self.host = host

    def get_dataset(self, dataset, resource, index_col=cpgintegrate.SUBJECT_ID_FIELD_NAME) -> pandas.DataFrame:
        resource_list = requests.get(
            url=self.host + '/api/3/action/package_show',
            headers={"Authorization": self.auth},
            params={"id": dataset},
        ).json()['result']['resources']

        resource_url = next(res['url'] for res in resource_list if res['name'] == resource or res['id'] == resource)

        return (pandas
                .read_csv(requests.get(resource_url, headers={"Authorization": self.auth}, stream=True).raw)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: resource_url})
                .pipe(lambda df: df.set_index(index_col) if index_col and index_col in df.columns else df)
                )

    def iter_files(self, dataset, resource_selector=lambda x: True) -> typing.Iterator[typing.IO]:
        resource_list = requests.get(
            url=self.host + '/api/3/action/package_show',
            headers={"Authorization": self.auth},
            params={"id": dataset},
        ).json()['result']['resources']

        for resource in resource_list:
            file = requests.get(resource['url'], headers={"Authorization": self.auth}, stream=True).raw
            setattr(file, cpgintegrate.SOURCE_FIELD_NAME, resource['url'])
            yield file
