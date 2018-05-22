import requests
from lxml import etree
import typing
from .connector import FileDownloadingConnector
import cpgintegrate
from cpgintegrate import ColumnInfoFrame
from bs4 import BeautifulSoup
import re
from zipfile import ZipFile
from io import BytesIO
from requests.utils import unquote

class OpenClinica(FileDownloadingConnector):

    def __init__(self, schema: str, auth: (str, str) = None, xml_path: str = None, dataset_id: int = None,
                 host='http://localhost/OpenClinica', **kwargs):
        super().__init__(**kwargs)
        self.base_url = host
        self.study_oid = schema

        self.xml, self.nsmap = None, None
        self.session = None
        self.xml_path = xml_path
        self.dataset_id = dataset_id

        if auth:
            self.session = requests.Session()
            username, password = auth
            self.session.post('/'.join([self.base_url, 'j_spring_security_check']),
                              data={'j_username': username, 'j_password': password})

            # Change to the right study via study name from metadata and parsing the ChangeStudy page
            study_name = self.session.get('/'.join([self.base_url, 'rest/metadata/json/view/%s/*/*' % schema])) \
                .json()['Study']['GlobalVariables']['StudyName']
            change_study_page = BeautifulSoup(self.session.get(self.base_url + '/ChangeStudy').content, 'lxml')
            study_id = change_study_page.find('form', action="ChangeStudy").find(
                string=re.compile('^' + study_name + " \(")).parent.parent.find('input')['value']
            self.session.post(self.base_url + '/ChangeStudy',
                              data={'studyId': study_id, 'action': 'submit', 'Submit': 'Confirm'})

    def _xpath(self, xpath):
        return self.xml.xpath(xpath, namespaces=self.nsmap)

    def _get_xml(self):
        if self.xml is None:
            if self.xml_path:
                self.xml = etree.parse(self.xml_path).getroot()
            elif self.dataset_id:
                dl_page = BeautifulSoup(self.session.get(
                    self.base_url + '/ExportDataset?datasetId=' + str(self.dataset_id)).content, 'lxml')
                zip_file_url = self.base_url + '/' + dl_page.find_all("a", href=re.compile('AccessFile'))[0].attrs[
                    'href']
                zip_file = ZipFile(BytesIO(self.session.get(zip_file_url).content))
                self.xml = etree.parse(zip_file.open(zip_file.namelist()[0])).getroot()
            else:
                self.xml = etree.fromstring(self.session.get(
                    self.base_url + '/rest/clinicaldata/xml/view/%s/*/*/*?includeAudits=y&includeDNs=y'
                    % self.study_oid).content)

            self.nsmap = self.xml.nsmap
            self.nsmap['d'] = self.nsmap.pop(None)

    def iter_files(self, item_oid: str) -> typing.Iterator[typing.IO]:
        self._get_xml()
        items = self._xpath(".//d:FormData[@OpenClinica:Status != 'invalid']"
                            "/d:ItemGroupData"
                            "/d:ItemData[@ItemOID = '%s']"
                            % item_oid)
        for item in items:
            if item.attrib['Value'] != "":
                resp = self.session.get(self.base_url + "/DownloadAttachedFile", stream=True,
                                        params={'fileName': item.attrib['Value']})
                file_like = resp.raw
                file_like.decode_content = True
                file_like.name = unquote(resp.url)
                setattr(file_like, cpgintegrate.SUBJECT_ID_ATTR,
                        item.xpath('ancestor::d:SubjectData', namespaces=self.nsmap)[0]
                        .get('{%s}StudySubjectID' % self.nsmap['OpenClinica']))
                yield file_like

    def list_datasets(self) -> set:
        if self.xml:
            return {'_'.join(elem.attrib['FormOID'].split('_')[:-1])
                    for elem in self.xml.xpath('.//d:FormData', namespaces=self.nsmap)}
        else:
            return {'_'.join(elem['@OID'].split('_')[:-1]) for elem in
                    self.session.get(self.base_url + '/rest/metadata/json/view/%s/*/*' % self.study_oid)
                        .json()['Study']['MetaDataVersion']['FormDef']
                    }

    def get_dataset(self, form_oid_prefix: str = "", include_meta_columns=False):

        def item_col_name(item_data):
            return "_".join(
                filter(None, [
                    self._xpath("./d:Study/d:MetaDataVersion/d:ItemDef[@OID='%s']"
                                % item_data.attrib['ItemOID'])[0].attrib['Name'],
                    item_data.getparent().attrib.get('ItemGroupRepeatKey')]))

        def form_to_dict(form):
            def item_group_listize(item_group):
                return (
                    (item_col_name(item_data),
                     item_data.attrib.get('Value'))
                    for item_data in item_group.xpath('./d:ItemData', namespaces=self.nsmap))

            def attribute_dictize(xml_item):
                return {
                    '%s:%s' % (etree.QName(xml_item.tag).localname, etree.QName(attrib).localname): value
                    for attrib, value in xml_item.attrib.items()}

            return dict(
                **attribute_dictize(form.find('../..')),
                **attribute_dictize(form.find('..')),
                **attribute_dictize(form),
                **{k: v for item_group in form.xpath('./d:ItemGroupData', namespaces=self.nsmap)
                   for k, v in item_group_listize(item_group)})

        def get_item_info(item_oid):
            item_info = {
                cpgintegrate.DESCRIPTION_ATTRIBUTE_NAME:
                    self._xpath(".//d:ItemDef[@OID='%s']" % item_oid)[0].attrib.get('Comment')}
            measurement_units = self.xml.xpath(".//d:ItemDef[@OID='%s']//d:MeasurementUnitRef"
                                               % item_oid, namespaces=self.nsmap)
            if len(measurement_units):
                item_info[cpgintegrate.UNITS_ATTRIBUTE_NAME] = \
                    self._xpath(".//d:MeasurementUnit[@OID='%s']"
                                % measurement_units[0].attrib.get('MeasurementUnitOID'))[0].attrib.get('Name')
            return item_info

        def source_from_frame(frame):
            return (self.base_url + '/rest/clinicaldata/html/print/' + self.study_oid + '/' +
                    frame['SubjectData:SubjectKey'].
                    str.cat([frame['StudyEventData:StudyEventOID'] +
                             ('%5B' + frame['StudyEventData:StudyEventRepeatKey'] +
                              '%5D' if 'StudyEventData:StudyEventRepeatKey' in frame.columns else ''),
                             frame['FormData:FormOID']], sep="/") + '?includeAudits=y&includeDNs=y')

        self._get_xml()

        forms = self._xpath(".//d:FormData[starts-with(@FormOID,'%s') and @OpenClinica:Status != 'invalid']"
                            % form_oid_prefix)

        columns = set((item_data.attrib['ItemOID'], item_col_name(item_data)) for item_data
                      in self._xpath(".//d:FormData[starts-with(@FormOID,'%s') and"
                                     " @OpenClinica:Status != 'invalid']//d:ItemData" % form_oid_prefix))

        column_info = {column_name: get_item_info(column_oid) for column_oid, column_name
                       in columns}

        column_order = {column_name:
                        self._xpath(".//d:ItemRef[@ItemOID='%s']" % column_oid)[0].sourceline
                        for column_oid, column_name
                        in columns}

        return (ColumnInfoFrame((form_to_dict(form) for form in forms), column_info=column_info)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: lambda frame: source_from_frame(frame)})
                .set_index('SubjectData:StudySubjectID', drop=True)
                .loc[:, lambda f: sorted([col for col in f.columns if
                                          (col in list(column_info.keys()) +
                                           ['FormData:Version', cpgintegrate.SOURCE_FIELD_NAME])
                                          or include_meta_columns],
                                         key=lambda col: column_order.get(col, 0))]
                .rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME)
                )
