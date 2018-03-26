# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

collect_ignore = [
    "setup.py",
    "docs/conf.py"
    ]

import pytest
from ontoma import OnToma

@pytest.fixture(scope="session")
def ontclient():
    '''the ontoma client, reusable between all tests'''
    return OnToma()

@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))