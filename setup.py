#!/usr/bin/env python
# encoding_ utf8

from setuptools import setup, find_packages
import codecs
import os
import re

def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
        name='ukis_kafka',
        version=find_version('ukis_kafka', "__init__.py"),
        description='This package implements a compact binary wireformat to stream vector features using apache kafka.',
        description_long=read('README'),
        author='Nico Mandery',
        author_email='nico.mandery@dlr.de',
        install_requires=[
            'Fiona>=1.7',
            'Shapely>=1.5',
            'msgpack-python>=0.4',
            'psycopg2>=2.6',
            'kafka-python>=1.3'
        ],
        packages=find_packages(exclude=['tests', 'tests.*', 'examples']),
        url='http://git.ukis.eoc.dlr.de/projects/BACKEND/repos/ukis_kafka'
)
