from setuptools import setup, find_packages
import os

setup(
    name="cpgintegrate",
    version="0.2.17",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.18.4',
        'pandas>=0.23.0',
        'xlrd',
        'sqlalchemy>=1.0',
        'beautifulsoup4',
        'lxml<5.0',
        'numpy',
        'scipy',
    ],
    extras_require={'dev': [
            'pytest>=3.2.2',
            'apache-airflow>=1.10.0',
        ],
        'win_auto': [
            'pywinauto',
            'patool',
        ],
                    },
    data_files=[
        (os.path.join(os.environ.get('AIRFLOW_HOME', 'airflow'), 'plugins'),
         ['cpgintegrate/airflow/cpg_airflow_plugin.py'])
    ],
)
