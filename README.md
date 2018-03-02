# OnToma


We want:

```python
from ontoma import find_efo

print(find_efo('asthma'))

#outputs:
'EFO:000270'
```

or the command line version

```sh
ontoma -i <input_file> -o <output_dir>
```

where input file is a file of diseases/traits in either codes or text

```txt
ICD9:720
asthma
alzheimer's
DO:124125
```

# Developing

## set up your environment

[Install pipenv](https://pipenv.readthedocs.io/en/latest/install/#homebrew-installation-of-pipenv) and then run
```sh
pipenv install --dev
```
to get all development dependencies installed. 

Test everything is working:
```sh
pipenv run pytest
```

## add a dependency with pipenv + setup.py
To add a dep for a library, add it by hand to `setup.py`, then add it separately to Pipfile, so that it shows up both as a transitive dependency and in your locked dev environment

## Release to PyPi
1. Once you are ready to cut a new release, update the version in setup.py and create a new git tag with git tag $VERSION.
2. Once you push the tag to GitHub with git push --tags a new CircleCI build is triggered.
3. You run a verification step to ensure that the git tag matches the version of ontoma that you added in step 1.
4. CircleCI performs all tests.
5. Once all of your test pass, you create a new Python package and upload it to PyPI using twine.

## TODO:

look into tokenizer
import OMIM logic

memoize/lru_cache the OBO requests
singleton implementation at module level - defer loading
oxo implementation
zooma
