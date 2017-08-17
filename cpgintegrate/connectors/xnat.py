import requests
import pandas
import pandas.io.json
import typing


class XNAT:
    def __init__(self, xnat_url: str, project_id: str, auth: (str, str)):
        self.base_url = xnat_url
        self.project_id = project_id
        self.session = requests.session()
        self.auth = auth

    def get_experiments(self) -> pandas.DataFrame:
        url = "/data/archive/projects/" + self.project_id + "/experiments"

        payload = {'guiStyle': 'true',
                   'columns': "label,subject_label," +
                              "xnat:imageSessionData/scanner/manufacturer,xnat:imageSessionData/scanner/model," +
                              "xnat:imageSessionData/scanner"
                   }

        return self._get_result_set(url, payload).set_index("subject_label")

    def iter_files(self, experiment_selector=lambda x: True,
                   scan_selector=lambda x: True,
                   image_selector=lambda x: True) -> typing.Iterator[typing.IO]:
        experiments = self.get_experiments()
        for subject_id, experiment in experiments[experiments.apply(experiment_selector, axis=1)].iterrows():
            scan_list = self._get_result_set(experiment.URI + "/scans")
            for _, scan in scan_list[scan_list.apply(scan_selector, axis=1)].iterrows():
                files = self._get_result_set(scan.URI+'/files')
                for _, file in files[files.apply(image_selector, axis=1)].iterrows():
                    file_url = self.base_url + file.URI
                    file = self.session.get(file_url, auth=self.auth, stream=True).raw
                    file.name = file_url
                    file.cpgintegrate_subject_id = subject_id
                    yield file

    def _get_result_set(self, url, params=None):
        url = self.base_url + url
        req = self.session.get(url, params=dict(**(params if params else {}), **{"format": "json"}), auth=self.auth)
        return pandas.io.json.json_normalize(req.json()['ResultSet']['Result'])
