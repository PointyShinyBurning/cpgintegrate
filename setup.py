from setuptools import setup, find_packages

setup(
    name="cpgintegrate",
    version="0.2.3",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.18.4',
        'lxml>=3',
        'pandas>=0.18.1',
        'xlrd',
        'sqlalchemy>=1.0',
    ]
)
