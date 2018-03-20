# -*- coding: utf-8 -*-
import os
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install

with open('VERSION') as version_file:
    VERSION = version_file.read().strip()

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)

setup(
    name='ontoma',
    version=VERSION,
    description='Ontology mapping for Open Targets',
    long_description=readme,
    author='Open Targets dev team',
    author_email='ops@opentargets.org',
    url='https://github.com/opentargets/OnToma',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords='opentargets ontology efo mapper',
    install_requires=[
        'requests',
        'obonet'
    ],
    python_requires='>=3.2',
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)

