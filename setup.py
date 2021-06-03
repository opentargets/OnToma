import os
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install

with open('VERSION') as version_file:
    VERSION = version_file.read().strip()

with open('README.md') as f:
    readme = f.read()


class VerifyVersionCommand(install):
    description = 'Verify that the git tag matches the VERSION file.'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')
        if tag != VERSION:
            sys.exit(f'Git tag {tag} does not match the version of this app {VERSION}')


setup(
    name='ontoma',
    version=VERSION,
    description='Ontology mapping for Open Targets',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Open Targets data team',
    author_email='data@opentargets.org',
    url='https://github.com/opentargets/OnToma',
    license='Apache License, Version 2.0',
    packages=find_packages(exclude=('tests', 'docs')),
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords='opentargets ontology efo mapper',
    install_requires=[
        'requests',
        'obonet',
        'click',
        'pronto',
        'pandas'
    ],
    entry_points='''
        [console_scripts]
        ontoma=ontoma.cli:ontoma
        ontoma-process-owl=ontoma.cli:ontoma_process_owl
    ''',
    python_requires='>=3.7',
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
