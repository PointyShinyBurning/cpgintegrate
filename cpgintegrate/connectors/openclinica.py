import requests
from lxml import etree
import pandas as pd


class OpenClinica:

    def __init__(self, openclinica_url: str, study_oid: str, subject_id_name: str = 'SubjectID'):
        self.base_url = openclinica_url
        self.study_oid = study_oid
        self.subject_id_name = subject_id_name
        self.xml = None
        self.session = None

    def login(self,  auth: (str, str)):
        self.session = requests.Session()
        username, password = auth
        self.session.post('/'.join([self.base_url, '/j_spring_security_check']),
                          data={'j_username': username, 'j_password': password})

    def fetch_xml(self, data_set_num: int = None):
        pass

    def get_dataset(self, xml=None, form_oid_prefix="") -> pd.DataFrame:
        if not xml:
            # TODO: xml download if not given
            print("xml download not yet implemented")
        tree = etree.parse(xml).getroot()
        nsmap = tree.nsmap
        nsmap['default'] = nsmap.pop(None)

        def form_to_dict(form):
            def item_group_listize(item_group):
                return (
                    ("_".join(filter(None, [item_data.attrib['ItemOID'], item_group.attrib.get('ItemGroupRepeatKey')])),
                     item_data.attrib.get('Value'))
                    for item_data in item_group.xpath('./default:ItemData', namespaces=nsmap))

            def attribute_dictize(xml_item):
                return {
                    '%s:%s' % (etree.QName(xml_item.tag).localname, etree.QName(attrib).localname): value
                    for attrib, value in xml_item.attrib.items()}
            return dict(
                **attribute_dictize(form.find('../..')),
                **attribute_dictize(form.find('..')),
                **attribute_dictize(form),
                **{k: v for item_group in form.xpath('./default:ItemGroupData', namespaces=nsmap)
                   for k, v in item_group_listize(item_group)})

        forms = tree.xpath(".//default:FormData[starts-with(@FormOID,'%s')]" % form_oid_prefix,
                           namespaces=nsmap)

        return (
            pd.DataFrame((form_to_dict(form) for form in forms)).
            set_index('SubjectData:StudySubjectID', drop=True).
            assign(
                Source=lambda frame:
                self.base_url + 'rest/clinicaldata/html/print/' + self.study_oid + '/'
                + frame['SubjectData:SubjectKey'].str.cat([
                    frame['StudyEventData:StudyEventOID'] +
                    '%5B' + frame.get('StudyEventData:StudyEventRepeatKey', "1") + '%5D',
                    frame['FormData:FormOID']], sep="/"
                ) + '?includeAudits=y&includeDNs=y',
            )
        )
