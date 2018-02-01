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

    def __init__(self, study_oid: str, auth: (str, str) = None, xml_path: str = None, dataset_id: int = None,
                 host="http://localhost/OpenClinica", **kwargs):
        super().__init__(**kwargs)
        self.base_url = host
        self.study_oid = study_oid

        self.xml, self.nsmap = None, None
        self.session = None

        if auth:
            self.session = requests.Session()
            username, password = auth
            self.session.post('/'.join([self.base_url, 'j_spring_security_check']),
                              data={'j_username': username, 'j_password': password})

            # Change to the right study via study name from metadata and parsing the ChangeStudy page
            study_name = self.session.get('/'.join([self.base_url, 'rest/metadata/json/view/%s/*/*' % study_oid])) \
                .json()['Study']['GlobalVariables']['StudyName']
            change_study_page = BeautifulSoup(self.session.get(self.base_url + '/ChangeStudy').content, 'lxml')
            study_id = change_study_page.find('form', action="ChangeStudy").find(
                string=re.compile('^' + study_name + " \(")).parent.parent.find('input')['value']
            self.session.post(self.base_url + '/ChangeStudy',
                              data={'studyId': study_id, 'action': 'submit', 'Submit': 'Confirm'})

        if xml_path:
            self.xml = etree.parse(xml_path).getroot()
        elif dataset_id:
            dl_page = BeautifulSoup(self.session.get(
                self.base_url + '/ExportDataset?datasetId=' + str(dataset_id)).content, 'lxml')
            zip_file_url = self.base_url + '/' + dl_page.find_all("a", href=re.compile('AccessFile'))[0].attrs['href']
            zip_file = ZipFile(BytesIO(self.session.get(zip_file_url).content))
            self.xml = etree.parse(zip_file.open(zip_file.namelist()[0])).getroot()
        else:
            # TODO Defer this download until iter_files or _read_dataset?
            self.xml = etree.fromstring(self.session.get(
                self.base_url + '/rest/clinicaldata/xml/view/%s/*/*/*?includeAudits=y&includeDNs=y'
                % study_oid).content)

        self.nsmap = self.xml.nsmap
        self.nsmap['default'] = self.nsmap.pop(None)

    def iter_files(self, item_oid: str) -> typing.Iterator[typing.IO]:
        items = self.xml. \
            xpath(".//default:FormData[@OpenClinica:Status != 'invalid']"
                  "/default:ItemGroupData"
                  "/default:ItemData[@ItemOID = '%s']"
                  % item_oid, namespaces=self.nsmap)
        for item in items:
            resp = self.session.get(self.base_url + "/DownloadAttachedFile", stream=True,
                                    params={"fileName": item.attrib['Value']})
            file_like = resp.raw
            file_like.decode_content = True
            file_like.name = resp.url
            setattr(file_like, cpgintegrate.SUBJECT_ID_ATTR,
                    item.xpath("ancestor::default:SubjectData", namespaces=self.nsmap)[0]
                    .get("{%s}StudySubjectID" % self.nsmap["OpenClinica"]))
            yield file_like

    def _read_dataset(self, form_oid_prefix: str = "", include_meta_columns=False):

        def form_to_dict(form):
            def item_group_listize(item_group):
                return (
                    ("_".join(filter(None, [item_data.attrib['ItemOID'], item_group.attrib.get('ItemGroupRepeatKey')])),
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
                                   namespaces=self.nsmap)[0].attrib.get("Comment")}
            measurement_units = self.xml.xpath(".//default:ItemDef[@OID='%s']//default:MeasurementUnitRef"
                                               % item_oid, namespaces=self.nsmap)
            if len(measurement_units):
                item_info[cpgintegrate.UNITS_ATTRIBUTE_NAME] = \
                    self.xml.xpath(".//default:MeasurementUnit[@OID='%s']"
                                   % measurement_units[0].attrib.get("MeasurementUnitOID"),
                                   namespaces=self.nsmap)[0].attrib.get("Name")
            return item_info

        def source_from_frame(frame):
            return (self.base_url + '/rest/clinicaldata/html/print/' + self.study_oid + '/' +
                    frame['SubjectData:SubjectKey'].
                    str.cat([frame['StudyEventData:StudyEventOID'] +
                             ('%5B' + frame['StudyEventData:StudyEventRepeatKey'] +
                              '%5D' if 'StudyEventData:StudyEventRepeatKey' in frame.columns else ""),
                             frame['FormData:FormOID']], sep="/") + '?includeAudits=y&includeDNs=y')

        forms = self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and @OpenClinica:Status != 'invalid']"
                               % form_oid_prefix, namespaces=self.nsmap)

        item_oids = {item.attrib.get('ItemOID') for item
                     in self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and"
                                       " @OpenClinica:Status != 'invalid']//default:ItemData"
                                       % form_oid_prefix, namespaces=self.nsmap)}

        column_info = {item_oid: get_item_info(item_oid) for item_oid in item_oids}

        return (ColumnInfoFrame((form_to_dict(form) for form in forms), column_info=column_info)
                .assign(**{cpgintegrate.SOURCE_FIELD_NAME: lambda frame: source_from_frame(frame)})
                .set_index('SubjectData:StudySubjectID', drop=True)
                .select(axis=1,
                        crit=(lambda col: True) if include_meta_columns
                        else (lambda col: (any(col.startswith(item_oid) for item_oid in item_oids)
                                           or col in ['FormData:Version', cpgintegrate.SOURCE_FIELD_NAME]))
                        )
                )
