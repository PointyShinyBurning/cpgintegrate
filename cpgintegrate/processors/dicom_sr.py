import pandas
import subprocess
import os
import tempfile
from lxml import etree
from cpgintegrate import ColumnInfoFrame
import cpgintegrate

def to_frame(file):
    """Turns DICOM structured reports into dataframes.

    Relies on dsr2xml from DCMTK (http://dicom.offis.de/dcmtk) being somewhere in the path

    :param file: A file-like of a dicom structured report
    :return: pandas DataFrame
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=".dcm", delete=False)
    temp_file.write(file.read())
    temp_file.close()

    item_repeats = {}
    xml = etree.fromstring(
            subprocess.check_output('dsr2xml -Ee +Ea +Wt -q -Ei "%s"' % temp_file.name, encoding="Latin-1",
                                    shell=True).encode('utf-8'))
    f = ColumnInfoFrame({'FileSubjectID': [xml.findtext('./patient/id')]})
    f = pandas.concat([f, ColumnInfoFrame(
        {prefix + '_' + l.tag: l.text
         for prefix in ['study', 'series'] for l in xml.find('./' + prefix)}, index=[0])],
                      axis=1)
    for elem in xml.iter("item"):
        if elem.get("valType") in ["NUM", "TEXT"]:
            identifier = elem.findtext("concept")
            value = elem.findtext("value")
            prefix = elem.xpath("../item[concept='Label' or concept='Region' or concept='Measure']/value") + elem.xpath(
                "../../item[concept='Finding Site']/value")
            suffixes = elem.xpath("./item[concept='Finding Site']/value") + elem.xpath(
                "./item[concept='Derivation']/value")
            unit = elem.xpath("./unit")
            if len(prefix):
                identifier = prefix[0].text.replace("�", "") + "_" + identifier

            for x in suffixes:
                identifier += "_" + x.text

            if identifier in item_repeats:
                item_repeats[identifier] += 1
                identifier = identifier + "_" + str(item_repeats[identifier])
            else:
                item_repeats[identifier] = 0

            if len(unit) and unit[0].text != 'no units':
                f.column_info[identifier] = {cpgintegrate.UNITS_ATTRIBUTE_NAME:unit[0].text}

            f[identifier] = value

    os.remove(temp_file.name)
    return f.set_index('FileSubjectID')
