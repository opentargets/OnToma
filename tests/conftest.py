# -*- coding: utf-8 -*-

import os
import sys

import pytest

from ontoma import OnToma

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

collect_ignore = [
    'setup.py',
    'docs/conf.py',
]


@pytest.fixture(scope="session")
def ontclient():
    """The ontoma client, reusable between all tests."""
    return OnToma(cache_dir='/tmp/efo_cache')


@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))
