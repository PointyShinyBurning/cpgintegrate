import requests
from lxml import etree
import pandas as pd
import typing
from .connector import Connector
import cpgintegrate
from cpgintegrate import ColumnInfoFrame


class OpenClinica(Connector):

    def __init__(self, openclinica_url: str, study_oid: str, auth: (str, str) = None, xml_path: str = None):
        super().__init__()
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

        # TODO Get xml from server if not provided, error-out if no xml and no/incorrect auth

    def iter_files(self, item_oid: str) -> typing.Iterator[typing.IO]:
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
            setattr(file_like, cpgintegrate.SUBJECT_ID_ATTR,
                    item.xpath("ancestor::default:SubjectData", namespaces=self.nsmap)[0]
                    .get("{%s}StudySubjectID" % self.nsmap["OpenClinica"]))
            yield file_like

    def _read_dataset(self, form_oid_prefix: str = "", include_meta_columns=False) -> pd.DataFrame:

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
                item_info[cpgintegrate.UNITS_ATTRIBUTE_NAME] =\
                    self.xml.xpath(".//default:MeasurementUnit[@OID='%s']"
                                   % measurement_units[0].attrib.get("MeasurementUnitOID"),
                                   namespaces=self.nsmap)[0].attrib.get("Name")
            return item_info

        forms = self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and @OpenClinica:Status != 'invalid']"
                               % form_oid_prefix, namespaces=self.nsmap)

        item_oids = {item.attrib.get('ItemOID') for item
                     in self.xml.xpath(".//default:FormData[starts-with(@FormOID,'%s') and"
                     " @OpenClinica:Status != 'invalid']//default:ItemData"
                                       % form_oid_prefix, namespaces=self.nsmap)}

        column_info = {item_oid: get_item_info(item_oid) for item_oid in item_oids}

        return (
            ColumnInfoFrame((form_to_dict(form) for form in forms), column_info=column_info)
            .set_index('SubjectData:StudySubjectID', drop=True)
            .assign(**{
                cpgintegrate.SOURCE_FIELD_NAME: lambda frame:
                self.base_url + '/rest/clinicaldata/html/print/' + self.study_oid + '/'
                + frame['SubjectData:SubjectKey'].str.cat([
                    frame['StudyEventData:StudyEventOID'] +
                    '%5B' + frame.get('StudyEventData:StudyEventRepeatKey', "1") + '%5D',
                    frame['FormData:FormOID']], sep="/"
                ) + '?includeAudits=y&includeDNs=y'},
            )
            .select(axis=1,
                    crit=lambda col: True if include_meta_columns
                    else lambda col: col in list(item_oids)+['FormData:Version', cpgintegrate.SOURCE_FIELD_NAME]
                    )
        )
