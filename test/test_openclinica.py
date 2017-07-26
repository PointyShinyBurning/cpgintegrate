from cpgintegrate.connectors import OpenClinica
import pkg_resources
import pandas

openclinica_url = 'http://localhost/OpenClinica'
study_oid = 'S_CPG_TEST'


def test_output():
    assert (
        OpenClinica(openclinica_url, study_oid,
                    xml_path=pkg_resources.resource_filename(__name__, "res/test_study.xml"))
        .get_dataset("F_GRIPSTRENGTH")
        .equals(pandas.read_pickle(pkg_resources.resource_filename(__name__, "res/test_grips_out.pkl")))
    )
