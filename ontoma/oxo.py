"""Wrapper for the OXO API."""

import logging

import requests


__all__ = [
    'OxoClient'
]

OXO = 'https://www.ebi.ac.uk/spot/oxo/api'

logger = logging.getLogger(__name__)


class OxoClient:
    """OxO wrapper class."""

    def __init__(self, base_url=OXO.rstrip('/')):
        self._baseapi = base_url
        self._searchapi = base_url + '/search'

    @staticmethod
    def _pages(method, url, *args, **kwargs):
        """Generator expression to paginate through OxO results."""
        r = method(url, *args, **kwargs)
        r.raise_for_status()
        resp = r.json()
        yield resp
        if 'next' not in resp['_links']:
            return
        while 'next' in resp['_links']:
            logger.debug(f"Found next page at {resp['_links']['next']['href']}")
            r = method(resp['_links']['next']['href'], *args, **kwargs)
            r.raise_for_status()
            resp = r.json()
            yield resp

    def search(self, ids=None, mapping_target='EFO', distance=1):
        """Query OxO with given parameters and yield the found mappings one by one."""
        payload = {
            'ids': ids,
            'mappingTarget': mapping_target,
            'distance': str(distance)
        }
        logger.debug(f'Querying OxO /search with payload: {payload}')
        for page in self._pages(requests.post, self._searchapi, data=payload):
            logger.info('/search - Returned {} mappings'.format(len(page['_embedded']['searchResults'])))
            for mapping in page['_embedded']['searchResults']:
                yield mapping
