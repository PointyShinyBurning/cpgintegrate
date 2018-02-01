from setuptools import setup, find_packages

setup(
    name="cpgintegrate",
    version="0.2.3",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.18.4',
        'lxml>=4',
        'pandas>=0.22.0',
        'xlrd',
        'sqlalchemy>=1.0',
        'beautifulsoup4',
        'apache-airflow>=1.9.0'
    ]
)
