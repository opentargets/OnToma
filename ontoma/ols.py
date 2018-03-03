# -*- coding: utf-8 -*-
# borrowed from https://github.com/cthoyt/ols-client/blob/master/src/ols_client/client.py
# Removed ontology and term methods. 
# Adding details/parameters for all search methods
import logging
import time
import requests

from ontoma import URLS

__all__ = [
    'OlsClient'
]

logger = logging.getLogger(__name__)

api_suggest = '/api/suggest'
api_search = '/api/search'
api_select = '/api/select'


class OlsClient:
    """Wraps the functions to query the Ontology Lookup Service such that alternative base URL's can be used.
    
    >>> ols = OlsClient()
    >>> r = ols.search('asthma')
    >>> r['response']['docs'][0]['iri']
    'http://purl.obolibrary.org/obo/NCIT_C28397'

    >>> r = ols.search('asthma',ontology=['efo'],query_fields=['synonims'],field_list=['iri','label'])
    >>> r['response']['docs'][0]['iri']
    'http://purl.obolibrary.org/obo/NCIT_C28397'

    >>> r = ols.suggest('asthma', ontology=['efo','ordo','hpo'])
    >>> r['response']['docs'][0]['autosuggest']
    u'asthma'

    >>> r= ols.select('asthma', ontology=['efo','ordo','hpo'])
    >>> r['response']['docs'][0]['iri']
    u'http://purl.obolibrary.org/obo/NCIT_C28397'

    >>> [x['short_form'] for x in t.select('alzheimer')['response']['docs'][:2]]
    [u'NCIT_C2866', u'NCIT_C38778']
    """

    def __init__(self, ols_base=None):
        """
        :param ols_base: An optional, custom URL for the OLS RESTful API.
        """
        self.base = (ols_base if ols_base is not None else URLS.OLS).rstrip('/')

        self.ontology_suggest = self.base + api_suggest
        self.ontology_select = self.base + api_select
        self.ontology_search = self.base + api_search


    def search(self, name, query_fields=None, ontology=None, 
                    field_list=None, facet=False, hl=False):
        """Searches the OLS with the given term
        :param str name:
        :param list[str] query_fields: Fields to query

        By default the search is performed over term labels, synonyms, 
        descriptions, identifiers and annotation properties.
        Specify the fields to query, the defaults are 
        {label, synonym, description, short_form, obo_id, annotations, logical_description, iri}

        :param list[str] ontology: list of ontologies id
        :return: list
        """
        params = {'q': name}
        params['facet'] = 'true' if facet else 'false'

        if ontology:
            params['ontology'] = ','.join(ontology)
        if query_fields:
            params['queryFields'] = ','.join(query_fields)
        if field_list:
            params['fieldList'] = ','.join(field_list)
        r = requests.get(self.ontology_search, params=params)
        r.raise_for_status()
        if r.json()['response']['numFound']:
            return r.json()['response']['docs']
        else:
            logger.debug('OLS returned empty response for {}'.format(name))
            return None

    def suggest(self, name, ontology=None):
        """Suggest terms from an optional list of ontologies
        :param str name:
        :param list[str] ontology:
        :rtype: dict
        .. seealso:: https://www.ebi.ac.uk/ols/docs/api#_suggest_term
        """
        params = {'q': name}
        if ontology:
            params['ontology'] = ','.join(ontology)
        response = requests.get(self.ontology_suggest, params=params)

        return response.json()

    def select(self, name, ontology=None, type=None):
        """Select terms
        Tuned specifically to support applications such as autocomplete.
        
        .. seealso:: https://www.ebi.ac.uk/ols/docs/api#_select
        """
        params = {'q': name}
        if ontology:
            params['ontology'] = ','.join(ontology)
        response = requests.get(self.ontology_select, params=params)

        return response.json()

    