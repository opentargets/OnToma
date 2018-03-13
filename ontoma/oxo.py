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
        logger.error(payload)
        try:
            for page in self._pages(requests.post, self._searchapi, data=payload):
                logger.debug('Returned {} mappings'.format(len(page['_embedded']['searchResults']))
                for mapping in page['_embedded']['searchResults']:
                    yield mapping
        except HTTPError as e:
            logger.error(e.response.json()['message'])
            return None




        # return r.json()['_embedded']['searchResults']
        #     while oxo.json().get('next') == True:
    
    def get_mappings(input_source = "ICD9CM", mapping_target='EFO'):
        self.search(input_source=input_source, mapping_target=mapping_target)
        # for row in oxomappings:
        #     self.icd9_to_efo[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']




