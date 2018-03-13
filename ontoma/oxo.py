# -*- coding: utf-8 -*-

"""
wrapper for the OXO api 
(reverse engineered! argh!)
"""

import logging
import time
import requests
from requests import HTTPError


__all__ = [
    'OxoClient'
]

OXO = 'https://www.ebi.ac.uk/spot/oxo/api'
MAXSIZE = 500

logger = logging.getLogger(__name__)




class OxoClient:
    ''' OXO wrapper class

    >>> oxo = OxoClient()
    >>> len(oxo._sources)
    940

    >>> list(oxo.search(input_source="ICD9CM"))[:1]
    [{'queryId': None, 'querySource': 'ICD9CM', 'curie': 'ICD9CM:730.92', 'label': '', 'mappingResponseList': [{'curie': 'EFO:0003102', 'label': 'osteomyelitis', 'sourcePrefixes': ['EFO'], 'targetPrefix': 'EFO', 'distance': 1}], '_links': {'self': {'href': 'https://www.ebi.ac.uk/spot/oxo/api/terms/ICD9CM:730.92'}, 'mappings': {'href': 'https://www.ebi.ac.uk/spot/oxo/api/mappings?fromId=ICD9CM:730.92'}}}]

    >>> for r in oxo.search(ids=['ICD9CM:171.6'],input_source="ICD9CM"):
    ...     print(r['label'])
    Malignant neoplasm of connective and other soft tissue of pelvis

    >>> oxo.search(input_source="ICD9CM", distance=2)
    '''

    def __init__(self, base_url=OXO.rstrip('/')):
        self._baseapi = base_url
        self._searchapi = base_url + '/search'
        self._mappingsapi = base_url + '/mappings'
        self._datasourcesapi = base_url + '/datasources'

        self.get_data_sources()


    @staticmethod
    def _pages(method, url, sleep = None, *args, **kwargs):
        '''generator expression to paginate through OXO methods
        '''
        r = method(url,*args,**kwargs)
        r.raise_for_status()
        resp = r.json()
        yield resp
        if 'next' not in resp['_links']:
            return
        while 'next' in resp['_links']:
            if sleep:
                time.sleep(sleep)
            logger.debug(resp['_links']['next']['href'])
            r = method(resp['_links']['next']['href'],*args,**kwargs)
            r.raise_for_status()
            resp = r.json()
            yield resp


    def get_data_sources(self):
        self._sources = set()
        payload = {'size': MAXSIZE}
        for p in self._pages(requests.get, self._datasourcesapi, params=payload):
            self._sources.update([x['prefix'] for x in p['_embedded']['datasources']])
        return


    
    def search(self, ids = None, input_source = None, mapping_target = 'EFO', distance = 1, size = MAXSIZE):
        '''iterates over the mappings, each being a dict with the following keys:
        [
        "_links",
        "curie",
        "label",
        "mappingResponseList",
        "queryId",
        "querySource"
        ]
        '''
        assert input_source in self._sources
        if size > MAXSIZE:
            raise ValueError('Maximum size is {}. Given: {}'.format(MAXSIZE,size))

        payload = {
            "size": size,
            "ids": ids,
            'inputSource': input_source,
            'mappingTarget': mapping_target,
            'distance': str(distance)
        }
        logger.debug('Passing parameters: {}'. format(payload))
        try:
            for page in self._pages(requests.post, self._searchapi, data=payload):
                logger.debug('Returned {} mappings'.format(len(page['_embedded']['searchResults'])))
                for mapping in page['_embedded']['searchResults']:
                    yield mapping
        except HTTPError as e:
            logger.error(e.response.json()['message'])
            return None


    
    def make_mappings(self, input_source = "ICD9CM", mapping_target='EFO'):
        src = self.search(input_source=input_source, mapping_target=mapping_target)
        mappings = {}
        for row in src:
            mappings[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']
        return mappings





