# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='OnToma',
    version='0.0.1',
    description='Ontology mapping for Open Targets',
    long_description=readme,
    author='Open Targets dev team',
    author_email='ops@opentargets.org,
    url='https://github.com/opentargets/OnToma',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

