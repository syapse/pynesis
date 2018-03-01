#!/usr/bin/env python
import platform

from setuptools import setup, find_packages

python_version = platform.python_version().rsplit(".", 1)[0]


install_requires = [
    "boto3>=1.4.0",
    "six>=1.9.0",
]

if python_version < "3.0":
    install_requires.append("simplejson>=3.0.0")

if python_version < "3.5":
    install_requires.append("typing>=3.6.1")

setup(
    name="pynesis",
    version="2.2.3",
    author="Matias Surdi",
    author_email="matias@surdi.net",
    keywords=["kinesis", "aws"],
    url="https://s3-us-west-1.amazonaws.com/dist.syapse.com/internal_python_packages/pynesis-2.2.3.tar.gz",
    description="Python high level client for Kinesis streams",
    long_description="Kinesis based python eventbus",
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    platforms="any",
    install_requires=install_requires,
    tests_require=["tox"],
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
)
