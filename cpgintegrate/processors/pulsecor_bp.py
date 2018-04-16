import pandas
from lxml import etree


def to_frame(file):
    parser = etree.XMLParser(recover=True)
    xml = etree.parse(file, parser)
    sheet = pandas.DataFrame(index=[0])
    for item in xml.findall(".//MeasDataLogger")[0]:
        sheet[item.tag] = item.text

    for item in xml.findall(".//Result")[0]:
        sheet[item.tag] = item.text

    return sheet
