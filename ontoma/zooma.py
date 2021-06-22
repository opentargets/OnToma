"""ZOOMA API wrapper."""

import logging

import requests

ZOOMA_API_URL = 'https://www.ebi.ac.uk/spot/zooma/v2/api'

__all__ = [
    'ZoomaClient'
]

logger = logging.getLogger(__name__)


class ZoomaClient:
    """Simple client to query ZOOMA. Will look in all curated datasources and perform a fuzzy search in EFO. Only HIGH
    quality mappings are considered."""

    def __init__(self, zooma_base=ZOOMA_API_URL):
        """
        :param zooma_base: An optional, custom URL for the Zooma RESTful API.
        """
        self.base = zooma_base.rstrip('/')
        self.session = requests.Session()

    def search(self, query_string):
        """Query ZOOMA and return all high confidence mappings."""
        payload = {
            'ontologies': '[EFO]',
        }
        url = f'{self.base}/services/annotate?propertyValue={query_string}'
        for zooma_result in requests.get(url, data=payload).json():
            if zooma_result['confidence'] == 'HIGH':
                for semantic_tag in zooma_result['semanticTags']:
                    yield semantic_tag
