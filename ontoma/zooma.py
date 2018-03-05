# -*- coding: utf-8 -*-

import logging
import time
import requests

ZOOMA = 'https://www.ebi.ac.uk/spot/zooma/v2/api'

__all__ = [
    'ZoomaClient'
]

logger = logging.getLogger(__name__)


class ZoomaClient:
    """simple client to query zooma

    By default (specifying nothing), Zooma will search its available databases 
    containing curated mappings (and that do not include ontology sources), 
    and if nothing is found it will look in the Ontology Lookup Service (OLS) 
    to predict ontology annotations.

    >>> z = ZoomaClient()
    >>> r = z.annotate("mus musculus")
    >>> r[0]['semanticTags']
    ['http://purl.obolibrary.org/obo/NCBITaxon_10090']
    >>> r[0]['confidence']
    'HIGH'
    >>> z.besthit("mus musculus")
    'http://purl.obolibrary.org/obo/NCBITaxon_10090'
    """

    def __init__(self, zooma_base=None,
                required=None, preferred=None, ontologies='none'):
        """
        :param zooma_base: An optional, custom URL for the Zooma RESTful API.
        """
        self.base = (zooma_base if zooma_base else ZOOMA).rstrip('/')
        self._annotate = self.base + '/services/annotate'

    def highconfhits(self, name):
        return [m for m in self.annotate(name) if m['confidence'] == 'HIGH']
    
    def besthit(self, name):
        return self.highconfhits(name)[0]['semanticTags'][0]


    @staticmethod
    def _make_filter_string(reqd, prefd, ontos):
        '''
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

        r = requests.get(self._annotate, params=params)
        logger.debug("Request to Zooma annotate API: {} - {}".format(r.status_code,name))
        r.raise_for_status()
        return r.json()




def request_to_zooma(self, property_value=None):
        '''
        Make a request to Zooma to get correct phenotype mapping and disease label
        :param property_value: Phenotype name from Genomics England
        :return: High confidence mappings .  Writes the output in the input file
        see docs: http://www.ebi.ac.uk/spot/zooma/docs/api.html
        '''
        #requests_cache.install_cache('zooma_results_cache_jan', backend='sqlite', expire_after=3000000)
        self._logger.info("Requesting")
        r = requests.get('http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate',
                             params={'propertyValue': property_value, 'propertyType': 'phenotype'})
        results = r.json()
        for item in results:
            if item['confidence'] == "HIGH":
                self.high_confidence_mappings[property_value] = {
                    'uri': item['_links']['olslinks'][0]['semanticTag'],
                    'label': item['derivedFrom']['annotatedProperty']['propertyValue'],
                    'omim_id': self.map_omim[property_value]
                }

            else:
                self.other_zooma_mappings[property_value] = {
                    'uri': item['_links']['olslinks'][0]['semanticTag'],
                    'label': item['derivedFrom']['annotatedProperty']['propertyValue'],
                    'omim_id': self.map_omim[property_value]
                }

        return self.high_confidence_mappings

# def use_zooma():
#         '''
#         Call request to Zooma function
#         :return: None.
#         '''


#         logger.info("use Zooma")
#         for phenotype in self.phenotype_set:
#             if phenotype:
#                 self._logger.info("Mapping '%s' with zooma..."%(phenotype))
#                 self.request_to_zooma(phenotype)

#         with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
#             tsv_writer = csv.writer(outfile, delimiter='\t')
#             for phenotype, value in self.high_confidence_mappings.items():
#                 tsv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

#         with open(Config.GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT, 'w') as map_file:
#             csv_writer = csv.writer(map_file, delimiter='\t')
#             for phenotype, value in self.other_zooma_mappings.items():
#                 csv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])
