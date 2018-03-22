
# -*- coding: utf-8 -*-

__all__ = [
    'OnToma'
]
from ontoma.interface import OnToma


import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)



