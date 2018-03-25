# -*- coding: utf-8 -*-

"""
OLS API wrapper

Original code borrowed from
 https://github.com/cthoyt/ols-client/blob/master/src/ols_client/client.py

- Removed ontology and term methods.
- Added details/parameters for all search methods
"""

import logging
import urllib.parse
import requests

OLS = 'http://www.ebi.ac.uk/ols'


__all__ = [
    'OlsClient'
]

logger = logging.getLogger(__name__)

API_SUGGEST = '/api/suggest'
API_SEARCH = '/api/search'
API_SELECT = '/api/select'
API_TERM = '/api/ontologies/{ontology}/terms/{iri}'
API_ANCESTORS = '/api/ontologies/{ontology}/terms/{iri}/ancestors'


def _concat_str_or_list(inputstr):
    '''always returns a comma joined list, whether the input is a
    single string or an iterable
    '''
    if type(inputstr) is str:
        return inputstr

    return ','.join(inputstr)


def _dparse(iri):
    '''double url encode the IRI, which is required
    '''
    return urllib.parse.quote_plus(urllib.parse.quote_plus(iri))


class OlsClient:
    """Wraps the functions to query the Ontology Lookup Service.

    >>> ols = OlsClient()
    >>> ols.search('asthma')[0]['iri']
    'http://purl.obolibrary.org/obo/NCIT_C28397'

    You can search in other ontologies and pass all other
    parameters accepted by OLS

    >>> ols.search('lung',ontology='uberon')[0]['iri']
    'http://purl.obolibrary.org/obo/UBERON_0002048'

    besthit() simply returns the first element:

    >>> ols.besthit('lung',ontology='uberon')['iri']
    'http://purl.obolibrary.org/obo/UBERON_0002048'

    `exact=True` forces an exact match:

    >>> ols.besthit('hypogammaglobulinemia',ontology='efo')['label']
    'Osteopetrosis - hypogammaglobulinemia'

    >>> ols.besthit('hypogammaglobulinemia',ontology='efo',exact=True) is None
    True

    >>> r = ols.search('asthma',ontology=['efo'],query_fields=['synonym'],field_list=['iri','label'])
    >>> r[0]['iri']
    'http://www.ebi.ac.uk/efo/EFO_0004591'

    Find the label of its first ancestor:

    >>> a = ols.get_ancestors('efo',r[0]['iri'])
    >>> a[0]['label']
    'asthma'


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

        self.ontology_suggest = self.base + API_SUGGEST
        self.ontology_select = self.base + API_SELECT
        self.ontology_search = self.base + API_SEARCH
        self.ontology_term = self.base + API_TERM
        self.ontology_ancestors = self.base + API_ANCESTORS


    def besthit(self, name, **kwargs):
        '''select first element of the /search API response
        '''
        searchresp = self.search(name, **kwargs)
        if searchresp:
            return searchresp[0]

        return None


    def get_term(self, ontology, iri):
        """Gets the data for a given term

        Args:
            ontology:   The name of the ontology
            iri:        The IRI of a term
        """

        url = self.ontology_term.format(ontology=ontology,
                                        iri=_dparse(iri))
        response = requests.get(url)
        return response.json()


    def get_ancestors(self, ont, iri):
        """Gets the data for a given term

        Args:
            ontology:   The name of the ontology
            iri:        The IRI of a term
        """
        url = self.ontology_ancestors.format(ontology=ont,
                                             iri=_dparse(iri))
        response = requests.get(url)
        try:
            return response.json()['_embedded']['terms']
        except KeyError as e:
            logger.error(response.json())
            import sys
            sys.exit(e)


    def search(self, name, query_fields=None, ontology=None, field_list=None,
               exact=None):
        """Searches the OLS with the given term

        Args:
            query_fields:   By default the search is performed over term labels,
                            synonyms, descriptions, identifiers and annotation
                            properties.
                            This option allows to specify the fields to query,
                            the defaults are
                            `{label, synonym, description, short_form, obo_id,
                            annotations, logical_description, iri}`
            exact:          Forces exact match if not `None`
        """
        params = {'q': name}

        if exact:
            params['exact'] = 'on'

        if ontology:
            params['ontology'] = _concat_str_or_list(ontology)
        elif self.ontology:
            params['ontology'] = _concat_str_or_list(self.ontology)

        if query_fields:
            params['queryFields'] = _concat_str_or_list(query_fields)
        elif self.query_fields:
            params['queryFields'] = _concat_str_or_list(self.query_fields)

        if field_list:
            params['fieldList'] = _concat_str_or_list(field_list)
        elif self.field_list:
            params['fieldList'] = _concat_str_or_list(self.field_list)

        req = requests.get(self.ontology_search, params=params)
        logger.debug("Request to OLS search API: %s - %s", req.status_code, name)
        req.raise_for_status()
        if req.json()['response']['numFound']:
            return req.json()['response']['docs']
        else:
            if exact:
                logger.debug('OLS exact search returned empty'
                             'response for %s', name)
            else:
                logger.debug('OLS search returned empty'
                             'response for %s', name)
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
            logger.debug('OLS suggest returned empty response for %s', name)
            return None


    def select(self, name, ontology=None, field_list=None):
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
            logger.debug('OLS select returned empty response for %s', name)
            return None
