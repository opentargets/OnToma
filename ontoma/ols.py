# -*- coding: utf-8 -*-

"""
OLS API wrapper

Original code borrowed from https://github.com/cthoyt/ols-client/blob/master/src/ols_client/client.py

- Removed ontology and term methods. 
- Added details/parameters for all search methods
"""

import logging
import time
import requests

OLS = 'http://www.ebi.ac.uk/ols'
    

__all__ = [
    'OlsClient'
]

logger = logging.getLogger(__name__)

api_suggest = '/api/suggest'
api_search = '/api/search'
api_select = '/api/select'


class OlsClient:
    """Wraps the functions to query the Ontology Lookup Service.
    
    >>> ols = OlsClient()
    >>> ols.search('asthma')[0]['iri']
    'http://purl.obolibrary.org/obo/NCIT_C28397'

    >>> r = ols.search('asthma',ontology=['efo'],query_fields=['synonym'],field_list=['iri','label'])
    >>> r[0]['iri']
    'http://www.ebi.ac.uk/efo/EFO_0004591'

    >>> r = ols.suggest('asthma', ontology=['efo','ordo','hpo'])
    >>> r[0]['autosuggest']
    'asthma'

    >>> r= ols.select('asthma', ontology=['efo','ordo','hpo'],field_list=['iri'])
    >>> r[0]['iri']
    'http://www.ebi.ac.uk/efo/EFO_0000270'

    >>> ols.search('Myofascial Pain Syndrome',ontology=['efo'])[0]['short_form']
    'EFO_1001054'

    >>> [x['short_form'] for x in ols.select('alzheimer')[:2]]
    ['NCIT_C2866', 'NCIT_C38778']

    You can also pass your favourite parameters at class instantiation:

    >>> ot_ols = OlsClient(ontology=['efo'],field_list=['short_form'])
    >>> ot_ols.search('asthma')[0]['short_form']
    'EFO_0000270'
    >>> ot_ols.besthit('asthma')['short_form']
    'EFO_0000270'
    """

    def __init__(self, ols_base=None, ontology=None, field_list=None, query_fields=None):
        """
        :param ols_base: An optional, custom URL for the OLS RESTful API.
        """
        self.base = (ols_base if ols_base else OLS).rstrip('/')

        self.ontology = ontology if ontology else None
        self.field_list = field_list if field_list else None
        self.query_fields = query_fields if query_fields else None

        self.ontology_suggest = self.base + api_suggest
        self.ontology_select = self.base + api_select
        self.ontology_search = self.base + api_search

    def besthit(self, name):
        try:
            return self.search(name)[0]
        except TypeError:
            #this is probably a None
            return self.search(name)

    def search(self, name, query_fields=None, ontology=None, field_list=None):
        """Searches the OLS with the given term

        By default the search is performed over term labels, synonyms, 
        descriptions, identifiers and annotation properties.
        Specify the fields to query, the defaults are 
        {label, synonym, description, short_form, obo_id, annotations, logical_description, iri}

        """
        params = {'q': name}

        if ontology:
            params['ontology'] = ','.join(ontology)
        elif self.ontology:
            params['ontology'] = ','.join(self.ontology)

        if query_fields:
            params['queryFields'] = ','.join(query_fields)
        elif self.query_fields:
            params['queryFields'] = ','.join(self.query_fields)

        if field_list:
            params['fieldList'] = ','.join(field_list)
        elif self.field_list:
            params['fieldList'] = ','.join(self.field_list)

        r = requests.get(self.ontology_search, params=params)
        r.raise_for_status()
        if r.json()['response']['numFound']:
            return r.json()['response']['docs']
        else:
            logger.debug('OLS search returned empty response for {}'.format(name))
            return None


    def suggest(self, name, ontology=None):
        """Suggest terms from an optional list of ontologies

        .. seealso:: https://www.ebi.ac.uk/ols/docs/api#_suggest_term
        """
        params = {'q': name}
        if ontology:
            params['ontology'] = ','.join(ontology)
        r = requests.get(self.ontology_suggest, params=params)
        r.raise_for_status()

        if r.json()['response']['numFound']:
            return r.json()['response']['docs']
        else:
            logger.debug('OLS suggest returned empty response for {}'.format(name))
            return None


    def select(self, name, ontology=None, type=None, field_list=None):
        """Select terms,
        Tuned specifically to support applications such as autocomplete.
        
        .. seealso:: https://www.ebi.ac.uk/ols/docs/api#_select
        """
        params = {'q': name}
        if ontology:
            params['ontology'] = ','.join(ontology)
        if field_list:
            params['fieldList'] = ','.join(field_list)
        r = requests.get(self.ontology_select, params=params)
        r.raise_for_status()

        if r.json()['response']['numFound']:
            return r.json()['response']['docs']
        else:
            logger.debug('OLS select returned empty response for {}'.format(name))
            return None

    