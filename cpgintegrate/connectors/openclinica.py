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

    def _get_xml(self):
        if not self.xml:
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
            self.nsmap['default'] = self.nsmap.pop(None)

    def iter_files(self, item_oid: str) -> typing.Iterator[typing.IO]:
        self._get_xml()
        items = self.xml. \
            xpath(".//default:FormData[@OpenClinica:Status != 'invalid']"
                  "/default:ItemGroupData"
                  "/default:ItemData[@ItemOID = '%s']"
                  % item_oid, namespaces=self.nsmap)
        for item in items:
            resp = self.session.get(self.base_url + "/DownloadAttachedFile", stream=True,
                                    params={'fileName': item.attrib['Value']})
            file_like = resp.raw
            file_like.decode_content = True
            file_like.name = resp.url
            setattr(file_like, cpgintegrate.SUBJECT_ID_ATTR,
                    item.xpath('ancestor::default:SubjectData', namespaces=self.nsmap)[0]
                    .get('{%s}StudySubjectID' % self.nsmap['OpenClinica']))
            yield file_like

    def list_datasets(self) -> set:
        if self.xml:
            return {'_'.join(elem.attrib['FormOID'].split('_')[:-1])
                    for elem in self.xml.xpath('.//default:FormData', namespaces=self.nsmap)}
        else:
            return {'_'.join(elem['@OID'].split('_')[:-1]) for elem in
                    self.session.get(self.base_url + '/rest/metadata/json/view/%s/*/*' % self.study_oid)
                        .json()['Study']['MetaDataVersion']['FormDef']
                    }

    def _read_dataset(self, form_oid_prefix: str = "", include_meta_columns=False):

        def item_col_name(item_data):
            return "_".join(
                filter(None, [
                    self.xml.xpath("./default:Study/default:MetaDataVersion/default:ItemDef[@OID='%s']"
                                   % item_data.attrib['ItemOID'], namespaces=self.nsmap)[0].attrib['Name'],
                    item_data.getparent().attrib.get('ItemGroupRepeatKey')]))

        def form_to_dict(form):
            def item_group_listize(item_group):
                return (
                    (item_col_name(item_data),
                     item_data.attrib.get('Value'))
                    for item_data in item_group.xpath('./default:ItemData', namespaces=self.nsmap))

            def attribute_dictize(xml_item):
                return {
                    '%s:%s' % (etree.QName(xml_item.tag).localname, etree.QName(attrib).localname): value
                    for attrib, value in xml_item.attrib.items()}

            return dict(
                **attribute_dictize(form.find('../..')),
                **attribute_dictize(form.find('..')),
                **attribute_dictize(form),
                **{k: v for item_group in form.xpath('./default:ItemGroupData', namespaces=self.nsmap)
                   for k, v in item_group_listize(item_group)})

        def get_item_info(item_oid):
            item_info = {
                cpgintegrate.DESCRIPTION_ATTRIBUTE_NAME:
                    self.xml.xpath(".//default:ItemDef[@OID='%s']" % item_oid,
                                   namespaces=self.nsmap)[0].attrib.get('Comment')}
            measurement_units = self.xml.xpath(".//default:ItemDef[@OID='%s']//default:MeasurementUnitRef"
                                               % item_oid, namespaces=self.nsmap)
            if len(measurement_units):
                item_info[cpgintegrate.UNITS_ATTRIBUTE_NAME] = \
                    self.xml.xpath(".//default:MeasurementUnit[@OID='%s']"
                                   % measurement_units[0].attrib.get('MeasurementUnitOID'),
                                   namespaces=self.nsmap)[0].attrib.get('Name')
            return item_info

        def source_from_frame(frame):
            return (self.base_url + '/rest/clinicaldata/html/print/' + self.study_oid + '/' +
                    frame['SubjectData:SubjectKey'].
                    str.cat([frame['StudyEventData:StudyEventOID'] +
                             ('%5B' + frame['StudyEventData:StudyEventRepeatKey'] +
                              '%5D' if 'StudyEventData:StudyEventRepeatKey' in frame.columns else ''),
                             frame['FormData:FormOID']], sep="/") + '?includeAudits=y&includeDNs=y')

        self._get_xml()

        forms = self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and @OpenClinica:Status != 'invalid']"
                               % form_oid_prefix, namespaces=self.nsmap)

        columns = set((item_data.attrib['ItemOID'], item_col_name(item_data)) for item_data
                      in self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and"
                                        " @OpenClinica:Status != 'invalid']//default:ItemData"
                                        % form_oid_prefix, namespaces=self.nsmap))

        column_info = {column_name: get_item_info(column_oid) for column_oid, column_name
                       in columns}

        return (ColumnInfoFrame((form_to_dict(form) for form in forms), column_info=column_info)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: lambda frame: source_from_frame(frame)})
                .set_index('SubjectData:StudySubjectID', drop=True)
                .select(axis=1,
                        crit=(lambda col: True) if include_meta_columns
                        else (lambda col: (col in column_info.keys()
                                           or col in ['FormData:Version', cpgintegrate.SOURCE_FIELD_NAME]))
                        )
                )
