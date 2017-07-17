from setuptools import setup, find_packages

setup(
    name="cpgdataintegrator",
    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.10',
        'lxml>=3',
        'pandas>=0.18.1',
    ]
)
