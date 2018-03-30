'''
Functions that download mapping flat-files from github repo or other
repositories
'''

__all__ = [
    "get_ot_zooma_to_efo_mappings",
    "get_omim_to_efo_mappings"
    ]

import csv
import logging
import requests
from ontoma.constants import URLS #pylint: disable=unused-import

logger = logging.getLogger(__name__)

def get_omim_to_efo_mappings(url):
    '''returns a dictionary that maps OMIM codes to EFO_uri
    >>> d = get_omim_to_efo_mappings(URLS['OMIM_EFO_MAP'])
    >>> d['609909']
    [{'iri': 'http://www.orpha.net/ORDO/Orphanet_154', 'label': 'Familial isolated dilated cardiomyopathy'}, {'iri': 'http://www.orpha.net/ORDO/Orphanet_217607', 'label': 'Familial dilated cardiomyopathy'}]
    '''
    mappings = {}
    logger.debug("OMIM to EFO mappings - requesting from URL %s", url)
    with requests.get(url, stream=True) as req:
        for i, row in enumerate(csv.DictReader(req.iter_lines(decode_unicode=True), delimiter='\t')):
            if row['OMIM'] not in mappings:
                mappings[row['OMIM']] = []
            mappings[row['OMIM']].append({'iri':row['efo_uri'],'label':row['efo_label']})
        logger.info("OMIM to EFO mappings - Parsed %s rows", i)
    return mappings


def get_ot_zooma_to_efo_mappings(url):
    '''download zooma and returns a dict
    >>> d = get_ot_zooma_to_efo_mappings(URLS['ZOOMA_EFO_MAP'])
    >>> d['skeletal dysplasias']
    'http://purl.obolibrary.org/obo/HP_0002652'
    '''
    mappings = {}
    logger.debug("ZOOMA to EFO mappings - requesting from URL %s", url)
    with requests.get(url, stream=True) as req:
        for i, row in enumerate(csv.DictReader(req.iter_lines(decode_unicode=True), delimiter='\t')):
            #(study, bioentity, property_type, property_value,
            # semantic_tag, annotator, annotation_date)
            # Note here should be 1:1 correspondence
            mappings[row['PROPERTY_VALUE'].lower()] = row['SEMANTIC_TAG']
        logger.info("ZOOMA to EFO mappings - Parsed %s rows", i)
    return mappings
