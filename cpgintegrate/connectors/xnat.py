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
                   'columns': "label,xnat:subjectData/label," +
                              "xnat:imageSessionData/scanner/manufacturer,xnat:imageSessionData/scanner/model," +
                              "xnat:imageSessionData/scanner"
                   }

        return self._get_result_set(url, payload).set_index("subject_label")

    def iter_files(self) -> typing.Iterator[typing.IO]:
        # TODO Iter through some files here
        pass

    def _get_result_set(self, url, params=None):
        url = self.base_url + url
        req = self.session.get(url, params=dict(**(params if params else {}), **{"format": "json"}), auth=self.auth)
        return pandas.io.json.json_normalize(req.json()['ResultSet']['Result'])
