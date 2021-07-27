__all__ = ['OnToma']

import logging

from ontoma.interface import OnToma

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-8s - %(name)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
