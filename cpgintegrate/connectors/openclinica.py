import requests
from lxml import etree
import pandas as pd


class OpenClinica:

    def __init__(self, openclinica_url: str, study_oid: str, xml_path=None, auth: (str, str) = None):
        self.base_url = openclinica_url
        self.study_oid = study_oid

        self.xml, self.nsmap = None, None
        self.session = None

        if auth:
            self.session = requests.Session()
            username, password = auth
            self.session.post('/'.join([self.base_url, 'j_spring_security_check']),
                              data={'j_username': username, 'j_password': password})

        if xml_path:
            self.xml = etree.parse(xml_path).getroot()
            self.nsmap = self.xml.nsmap
            self.nsmap['default'] = self.nsmap.pop(None)

    def iter_files(self, item_oid: str):
        items = self.xml.\
            xpath(".//default:FormData[@OpenClinica:Status != 'invalid']"
                  "/default:ItemGroupData"
                  "/default:ItemData[@ItemOID = '%s']"
                  % item_oid, namespaces=self.nsmap)
        for item in items:
            resp = self.session.get(self.base_url+"/DownloadAttachedFile", stream=True,
                                    params={"fileName": item.attrib['Value']})
            file_like = resp.raw
            file_like.decode_content = True
            file_like.name = resp.url
            yield file_like

    def get_dataset(self, form_oid_prefix="") -> pd.DataFrame:

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

        forms = self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and @OpenClinica:Status != 'invalid']"
                               % form_oid_prefix, namespaces=self.nsmap)

        return (
            pd.DataFrame((form_to_dict(form) for form in forms)).
            set_index('SubjectData:StudySubjectID', drop=True).
            assign(
                Source=lambda frame:
                self.base_url + '/rest/clinicaldata/html/print/' + self.study_oid + '/'
                + frame['SubjectData:SubjectKey'].str.cat([
                    frame['StudyEventData:StudyEventOID'] +
                    '%5B' + frame.get('StudyEventData:StudyEventRepeatKey', "1") + '%5D',
                    frame['FormData:FormOID']], sep="/"
                ) + '?includeAudits=y&includeDNs=y',
            )
        )
