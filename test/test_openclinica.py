from cpgintegrate.connectors import OpenClinica
import pkg_resources
import pandas

openclinica_url = 'http://localhost/OpenClinica'
study_oid = 'S_CPG_TEST'


def test_ouput():
    assert (
        OpenClinica(openclinica_url, study_oid)
        .get_dataset(pkg_resources.resource_filename(__name__, "res/test_study.xml"), "F_GRIPSTRENGTH")
        .equals(pandas.read_pickle(pkg_resources.resource_filename(__name__, "res/test_grips_out.pkl")))
    )
