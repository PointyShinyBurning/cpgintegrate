# cpgintegrate
Is a Python package for turning the output of some medical research software into pandas dataframes and, via a (hacky, brittle) [Airflow](https://airflow.apache.org/) plugin, creating workflows that upload them to a [ckan](https://ckan.org/) instance as csvs, along with a bit of metadata.

It was written for the [SABRE study]('https://www.sabrestudy.org') at University College London and [that study's workflow](https://github.com/PointyShinyBurning/sabre_flow) is a good example of how it might be used.

**WARNING: Extremely far from being stable, or probably doing anything sensible at all except in my narrow use cases**

## installing
`pip install .` from the download directory or `pip install git+https://github.com/PointyShinyBurning/cpgintegrate.git#egg=cpgintegrate` to fetch the latest development version straight from the internet.

For the Airflow plugin you need to copy airflow/cpg_airflow_plugin.py to your `$AIRFLOW_HOME/plugins` folder (in some pip versions this might happen automatically)

## basic use
**Connectors** let you talk to a data capture system, for example [XNAT](https://www.xnat.org/). To get the list of sessions from the "Effects of bilingualism on brain structure and function" study on XNAT CENTRAL:
```
from cpgintegrate.connectors.xnat import XNAT

xnat_central = XNAT(schema='L2struc', host='https://central.xnat.org/')
sess_list = xnat_central.get_dataset()
sess_list.to_csv('sess_list.csv')
```
Some connectors also have a **process_files** method that lets you run a function (returning a DataFrame) on the files those systems contain and aggregate them. For example, the first five bytes from 10 files in the study:
```
import pandas
def initial_bytes(file):
    return pandas.DataFrame({"bytes": [file.read(5)]})

files_first_bytes = xnat_central.process_files(initial_bytes, limit=10)
files_first_bytes.to_csv('files_first_bytes.csv')
```
Some more useful ones for various file formats are in the cpgintegrate.processors package.