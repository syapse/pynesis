#!/usr/bin/env python

from setuptools import setup, find_packages

install_requires = [
    "boto3>=1.4.0",
    "six>=1.9.0",
    "simplejson>=3.0.0;python_version<'3.0'",
    "typing>=3.6.1;python_version<'3.5'",
]

setup(
    name="pynesis",
    version="1.0",
    url="https://github.com/ticketea/pynesis",
    description="Kinesis based python eventbus",
    long_description="Kinesis based python eventbus",
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    platforms="any",
    install_requires=install_requires,
    tests_require=["tox"],
)
