import requests
import pandas
import pandas.io.json
import typing
from .connector import FileDownloadingConnector
import cpgintegrate


class XNAT(FileDownloadingConnector):

    def __init__(self, schema: str, auth: (str, str)=(), host="https://localhost/xnat", **kwargs):
        super().__init__(**kwargs)
        self.base_url = host
        self.project_id = schema
        self.session = requests.session()
        self.auth = auth

    def get_dataset(self) -> pandas.DataFrame:
        url = "/data/archive/projects/" + self.project_id + "/experiments"

        payload = {'guiStyle': 'true',
                   'columns': "label,subject_label," +
                              "xnat:imageSessionData/scanner/manufacturer,xnat:imageSessionData/scanner/model," +
                              "xnat:imageSessionData/scanner"
                   }

        return (self._get_result_set(url, payload).set_index("subject_label")
                .rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME))

    def iter_files(self, experiment_selector=lambda x: True,
                   scan_selector=lambda x: True,
                   image_selector=lambda x: True,
                   iter_resources=False) -> typing.Iterator[typing.IO]:
        experiments = self.get_dataset()
        for subject_id, experiment in experiments[experiments.apply(experiment_selector, axis=1)].iterrows():
            scan_list =self._get_result_set(experiment.URI + "/resources") \
                if iter_resources else self._get_result_set(experiment.URI + "/scans")
            for __, scan in scan_list[scan_list.apply(scan_selector, axis=1)].iterrows():
                if iter_resources:
                    files = self._get_result_set(experiment.URI + '/resources/' + scan.xnat_abstractresource_id + '/files')
                else:
                    files = self._get_result_set(scan.URI+'/files')
                for _, file_elem in files[files.apply(image_selector, axis=1)].iterrows():
                    file_url = self.base_url + file_elem.URI
                    file = self.session.get(file_url, auth=self.auth, stream=True).raw
                    file.name = file_url
                    setattr(file, cpgintegrate.SUBJECT_ID_ATTR, subject_id)
                    setattr(file, cpgintegrate.CACHE_KEY_ATTR, file.headers['Last-Modified'])
                    yield file

    def _get_result_set(self, url, params=None):
        url = self.base_url + url
        req = self.session.get(url, params=dict(**(params if params else {}), **{"format": "json"}), auth=self.auth)
        return pandas.io.json.json_normalize(req.json()['ResultSet']['Result'])
