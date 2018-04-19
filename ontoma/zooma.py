# -*- coding: utf-8 -*-

'''
ZOOMA api wrapper
'''

import logging
import requests

ZOOMA = 'https://www.ebi.ac.uk/spot/zooma/v2/api'

__all__ = [
    'ZoomaClient'
]

logger = logging.getLogger(__name__)


class ZoomaClient:
    """Simple client to query zooma

    By default (specifying nothing), Zooma will search its available databases
    containing curated mappings (and that do not include ontology sources),
    and if nothing is found it will look in the Ontology Lookup Service (OLS)
    to predict ontology annotations.

    Example:
        >>> z = ZoomaClient()
        >>> r = z.annotate("mus musculus")
        >>> r[0]['semanticTags']
        ['http://purl.obolibrary.org/obo/NCBITaxon_10090']

        >>> r[0]['confidence']
        'HIGH'

        >>> z.besthit("mus musculus")['iri']
        'http://purl.obolibrary.org/obo/NCBITaxon_10090'
    """

    def __init__(self, zooma_base=None,
                required=None, preferred=None, ontologies='none'):
        """
        :param zooma_base: An optional, custom URL for the Zooma RESTful API.
        """
        self.base = (zooma_base if zooma_base else ZOOMA).rstrip('/')
        self.session = requests.Session()
        self._annotate = self.base + '/services/annotate'

    def highconfhits(self, name):
        return [m for m in self.annotate(name,ontologies='efo,hp')
                if m['confidence'] == 'HIGH']

    def besthit(self, name):
        try:
            zoomabest = self.highconfhits(name)[0]
            return {'iri': zoomabest['semanticTags'][0],
                    'label': zoomabest['annotatedProperty']['propertyValue']
            }
        except IndexError:
            logger.debug('Empty response from ZOoma API for {}'.format(name))
            return None


    @staticmethod
    def _make_filter_string(reqd, prefd, ontos):
        '''
        TODO: expand to deal with list input

        >>> z = ZoomaClient()
        >>> z._make_filter_string("atlas,gwas","gwas","efo")
        'required:[atlas,gwas],preferred:[gwas],ontologies:[efo]'
        >>> z._make_filter_string(None,None,"none")
        'ontologies:[none]'
        '''
        filters = []
        if reqd:
            filters.append('required:[{}]'.format(reqd))
        if prefd:
            filters.append('preferred:[{}]'.format(prefd))
        if ontos:
            filters.append('ontologies:[{}]'.format(ontos))

        return ','.join(filters)


    def annotate(self, name, property_type = None,
                required=None, preferred=None, ontologies='none'):

        params = {'propertyValue': name}

        if property_type: params['propertyType'] = property_type

        #The 'ontologies:[none]' parameter will restrain Zooma from looking in
        #the OLS if no annotation was found.
        params['filters'] = self._make_filter_string(required,preferred,ontologies)

        r = self.session.get(self._annotate, params=params)
        logger.debug("Request to Zooma annotate API: {} - {}".format(r.status_code,name))
        r.raise_for_status()
        return r.json()


