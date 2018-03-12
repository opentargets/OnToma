# -*- coding: utf-8 -*-

"""
wrapper for the OXO api 
(reverse engineered! argh!)
"""



import logging
import time
import requests


__all__ = [
    'OxoClient'
]

OXO = 'https://www.ebi.ac.uk/spot/oxo/api'

logger = logging.getLogger(__name__)


class OxoClient:
    ''' OXO wrapper class

    >>> oxo = OxoClient()
    >>> oxo.search(input_source="ICD9CM", distance=2)
    '''

    def __init__(self, base_url=OXO.rstrip('/')):
        self._baseapi = base_url
        self._searchapi = base_url + '/search'
        self._mappingsapi = base_url + '/mappings'
    
    def search(self, ids = None, input_source = None, mapping_target = 'EFO', distance = 1):
        payload = {
            "size": 500,
            "ids": ids,
            'inputSource': input_source,
            'mappingTarget': mapping_target,
            'distance': str(distance)
        }

        payload={"ids":ids,"inputSource":"ICD9CM","mappingTarget":["EFO"],"distance":"3"}
        r = requests.post(self._searchapi, data=payload)

        #TODO must paginate!
        #https://www.ebi.ac.uk/spot/oxo/api/search?page=1&size=500
        # Pagination - if there is a "next" link, update URL and read next page	
        # try:
        #     url = r.links['next']['url']
        # except KeyError:						# If there is no "next" link
        #     break

        return r.json()['_embedded']['searchResults']
        #     while oxo.json().get('next') == True:
        # for row in oxomappings:
        #     self.icd9_to_efo[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']



