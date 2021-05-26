![Documentation Status](https://readthedocs.org/projects/ontoma/badge/?version=stable) ![Build Status](https://img.shields.io/circleci/build/gh/opentargets/OnToma)

OnToma is a python module that helps you map your disease/phenotype terms to the
ontology we use in the Open Targets platform.

The ontology we use in the Open Targets platform is a subset (aka. _slim_) of
the EFO ontology _plus_ any HPO terms for which a valid EFO mapping could
not be found.


*features*

* Wrap OLS, OXO, Zooma in a pythonic API
* Always tries to output full URI
* Tries to find mappings iteratively using the faster methods first
* Checks if mapping is in the subset of EFO that gets included in the
Open Targets platform
* *tries to* follow the procedure highlighted in https://github.com/opentargets/data_release/blob/master/diagram.jpg


# Usage

## Installing

`pip install ontoma`

## Quickstart

Looking for a disease or phenotype string is simple:

```python
from ontoma import OnToma

otmap = OnToma()
print(otmap.find_term('asthma'))

#outputs:
'http://www.ebi.ac.uk/efo/EFO_0000270'
```

you can obtain more details using the `verbose` flag:

```python
print(otmap.find_term('asthma',verbose=True))

#outputs something similar to:
{'term':'http://www.ebi.ac.uk/efo/EFO_0000270','label':'asthma',
'source':'EFO OBO','quality':'match' ...}
```
**From the command line**

The command line version can be invoked with `ontoma`
(type `ontoma --help` to find out about the usage):

```sh
ontoma <input_file> <output_file>
```
where input file can be replaced with `-` to read from stdin and write to stdout.

Which means that to read from a previous command, using pipes:
```sh
echo 'asthma' | ontoma - <output_file>
```

will output a file `test.txt` containing the result, where it came from and the
degree of confidence of the match (one of {match, fuzzy, check}):

```
http://www.ebi.ac.uk/efo/EFO_0000270    asthma  EFO OBO     match
```

Piping also works for the output. If you want to find the string "mymatch" from
the results, you can:
```sh
ontoma <input_file> - | grep "mymatch"
```

More detailed documentation is at http://ontoma.readthedocs.io/en/stable/.

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

**if you don't like pipenv** you can stick with the more traditional
setuptools/virtualenv setup:

```sh
git clone https://github.com/opentargets/OnToma.git
virtualenv -p python3 venv
source venv/bin/activate
pip install --editable .
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
