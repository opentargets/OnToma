OnToma is a python module that helps you map your disease/phenotype terms to the
ontology we use in the Open Targets platform. 

# Usage

## Installing

`pip install ontoma`

## Quickstart

Basic usage should be simple:

```python
from ontoma import OnToma

otmap = OnToma()
print(otmap.find_efo('asthma'))

#outputs:
'EFO_000270'
```

or the command line version

```sh
ontoma -i <input_file> -o <output_dir>
```

where input file is a file of diseases/traits in either codes or text

```
ICD9:720
asthma
alzheimer's
DO:124125
```

More detailed documentation is at [![Documentation Status](https://readthedocs.org/projects/ontoma/badge/?version=latest)](http://ontoma.readthedocs.io/en/latest/?badge=latest)
http://ontoma.readthedocs.io/en/stable/

# Developing

## set up your environment
First clone this repo

```
git clone https://github.com/opentargets/OnToma.git
```

[Install pipenv](https://pipenv.readthedocs.io/en/latest/install/#homebrew-installation-of-pipenv) and then run
```sh
pipenv install --dev
```
to get all development dependencies installed. 

Test everything is working:
```sh
pipenv run pytest
```

## How to add a dependency

**Add to both pipenv AND setup.py**

To add a dep for a library, add it by hand to `setup.py`, then add it separately
to `Pipfile`, so that it shows up both as a transitive dependency and in your
locked dev environment

## Release to PyPi

Simply run `./bumpversion.sh`

The script will tag, push and trigger a new CI run. 
The package will be automatically uploaded to pypi. 

## TODO:

- [ ] memoize/lru_cache the OBO/__init__ file requests

see one of :
    https://docs.python.org/3/library/functools.html
    https://stackoverflow.com/questions/3012421/python-memoising-deferred-lookup-property-decorator
    https://stackoverflow.com/questions/17486104/python-lazy-loading-of-class-attributes
    https://stackoverflow.com/questions/14946264/python-lru-cache-decorator-per-instance
    singleton implementation at module level - defer loading

- [ ] command line interface