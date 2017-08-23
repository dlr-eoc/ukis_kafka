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
        name='ukis_streaming',
        version=find_version('ukis_streaming', "__init__.py"),
        description='This package implements a compact binary wireformat to stream vector features over messaging networks.',
        description_long=read('README.md'),
        author='Nico Mandery',
        author_email='nico.mandery@dlr.de',
        install_requires=[
            'Fiona>=1.7',
            'Shapely>=1.5',
            'msgpack-python>=0.4'
        ],
        packages=find_packages(exclude=['tests', 'tests.*']),
        url='http://git.ukis.eoc.dlr.de/projects/BACKEND/repos/ukis_streaming'
)
