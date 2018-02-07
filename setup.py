from setuptools import setup, find_packages
import os

setup(
    name="cpgintegrate",
    version="0.2.5",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.18.4',
        'pandas>=0.22.0',
        'xlrd',
        'sqlalchemy>=1.0',
        'beautifulsoup4',
        'apache-airflow>=1.9.0',
        'lxml',
    ],
    extras_require={'dev': [
            'pytest>=3.2.2',
        ]
                    },
    data_files=[
        (os.path.join(os.environ.get('AIRFLOW_HOME', 'airflow'), 'plugins'),
         ['cpgintegrate/airflow_plugin/cpg_airflow_plugin.py'])
    ],
)
