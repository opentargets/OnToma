# -*- coding: utf-8 -*-
__all__ = [
    "OnToma",
    "make_uri"
    ]

from ontoma.downloaders import get_omim_to_efo_mappings, get_ot_zooma_to_efo_mappings
from ontoma.ols import OlsClient
from ontoma.zooma import ZoomaClient
from ontoma.oxo import OxoClient

from ontoma.constants import URLS

import requests
import csv
import json
from io import BytesIO, TextIOWrapper
import obonet

import logging
logger = logging.getLogger(__name__)

# if you want to use across classes
# _cache = {"files":None}


def name_to_label_mapping(obonetwork):
    '''
    builds name <=> label lookup dictionaries starting
    from an OBO file
    '''
    id_to_name = {}
    name_to_id = {}
    for nodeid, data in obonetwork.nodes(data=True):
        id_to_name[nodeid] = data['name']
        name_to_id[data['name']] = nodeid 
        if 'synonym' in data:
            for s in data['synonym']:
                #some str.split voodoo is necessary to unpact the synonyms
                # as specified in the obo file
                name_to_id[s.split('\"')[1]] = nodeid
    return id_to_name, name_to_id


def make_uri(ontology_short_form):
    '''
    Transform a short form ontology code in a full URI. 
    Currently works for EFO, HPO, ORDO and MP. 

    Args:
        ontology_short_form: An ontology code in the short format, like 'EFO:0000270'.

    Returns:
        A full URI.

    Example:
        >>> make_uri('EFO:0000270')
        'http://www.ebi.ac.uk/efo/EFO_0000270'
        
        >>> make_uri('HP_0000270')
        'http://purl.obolibrary.org/obo/HP_0000270'
        
        >>> make_uri('http://purl.obolibrary.org/obo/HP_0000270')
        'http://purl.obolibrary.org/obo/HP_0000270'
    '''
    if ontology_short_form.startswith('http'): 
        return ontology_short_form
    ontology_code = ontology_short_form.replace(':',"_")
    if ontology_code.startswith('EFO'):
        return 'http://www.ebi.ac.uk/efo/'+ontology_code
    elif ontology_code.startswith('HP') or ontology_code.startswith('MP') :
        return 'http://purl.obolibrary.org/obo/' + ontology_code
    elif ontology_code.startswith('Orphanet') :
        return 'http://www.orpha.net/ORDO/' + ontology_code
    else:
        logger.error("Could not build an URI. {} not recognized".format(ontology_code))
        raise Exception


class OnToma(object):
    '''Open Targets ontology mapping wrapper

    The output should always be a EFO/OpenTargets ontology URI.

    Initialize the class (which will download EFO,OBO and others):
    >>> t=OnToma()

    We can now lookup "asthma" and get:
    >>> t.efo_lookup('asthma')
    'EFO:0000270'

    Search by synonyms coming from the OBO file is also supported
    >>> t.efo_lookup('Asthma unspecified')
    'EFO:0000270'

    Reverse lookups uses the get_efo_label() method
    >>> t.get_efo_label('EFO_0000270')
    'asthma'
    >>> t.get_efo_label('EFO:0000270')
    'asthma'


    Similarly, we can now lookup "Phenotypic abnormality" on HP: 
    >>> t.hp_lookup('Phenotypic abnormality')
    'HP:0000118'
    >>> t.hp_lookup('Narrow nasal tip')
    'HP:0011832'

    Lookup in OLS
    >>> t.ols_lookup('asthma')
    'EFO_0000270'

    OMIM code lookup
    >>> t.omim_lookup('230650')
    'http://www.orpha.net/ORDO/Orphanet_354'
    
    >>> t.zooma_lookup('asthma')
    'http://www.ebi.ac.uk/efo/EFO_0000270'


    Searching the ICD9 code for 'other dermatoses' returns EFO's skin disease:
    >>> t.icd9_lookup('696')
    'EFO:0000676'

    There is also a semi-intelligent wrapper, which tries to guess the 
    best matching strategy:
    >>> t.find_efo('asthma')
    'http://www.ebi.ac.uk/efo/EFO_0000270'
    >>> t.find_efo('615877',code='OMIM')
    'http://www.orpha.net/ORDO/Orphanet_202948'
    '''

    def __init__(self, efourl = URLS.EFO, 
                        hpurl = URLS.HP):

        self.logger = logging.getLogger(__name__)

        '''Parse the ontology obo files for exact match lookup'''
        #TODO delay download of the OBO files until the class instance is used
        self._efo = obonet.read_obo(efourl)
        self.logger.info('EFO parsed. Size: {} nodes'.format(len(self._efo)))
        self._hp = obonet.read_obo(hpurl)
        self.logger.info('HP parsed. Size: {} nodes'.format(len(self._hp)))

        '''Create name <=> label mappings'''
        self.efo_to_name, self.name_to_efo = name_to_label_mapping(self._efo)
        
        self.hp_to_name, self.name_to_hp = name_to_label_mapping(self._hp)
        

        '''Initialize API clients'''

        self._ols = OlsClient(ontology=['efo'],field_list=['short_form'])
        self._zooma = ZoomaClient()
        self._oxo = OxoClient()

        '''ICD9 <=> EFO mappings '''
        self._icd9_to_efo = self._oxo.make_mappings(input_source = "ICD9CM",
                                                   mapping_target= 'EFO')

        '''OT specific mappings in our github repos'''
        self._zooma_to_efo_map = get_ot_zooma_to_efo_mappings(URLS.ZOOMA_EFO_MAP)
        self._omim_to_efo = get_omim_to_efo_mappings(URLS.OMIM_EFO_MAP)


    def get_efo_label(self, efocode):
        '''given an EFO code, returns name/label
        '''
        try:
            return self.efo_to_name[efocode.replace('_',':')]
        except KeyError:
            logger.error('EFO ID {} not found'.format(efocode))
            return None

    def zooma_lookup(self, name):
        return self._zooma.besthit(name)

    def otzooma_map_lookup(self, name):
        '''NOTE: this is not a lookup to zooma service, but rather the manual 
        OpenTargets mapping we submitted to zooma.      
        '''
        return self._zooma_to_efo_map[name]

    def icd9_lookup(self, icd9code):
        return self._icd9_to_efo[icd9code]

    def omim_lookup(self, omimcode):
        #FIXME assumes the first is the best hit. is this ok?
        return self._omim_to_efo[omimcode][0]
    
    def ols_lookup(self, name):
        return self._ols.besthit(name)['short_form']

    def hp_lookup(self, name):
        return self.name_to_hp[name]
    
    def efo_lookup(self, name):
        return self.name_to_efo[name]

    def oxo_lookup(self, other_ontology_id, input_source="ICD9CM"):
        '''should return an EFO code for any given xref'''
        return self._oxo.search(ids=[other_ontology_id],
                                input_source=input_source,
                                mapping_target = 'EFO', 
                                distance = 2)


    def find_efo(self, query, code=None):
        '''Finds the most likely EFO code for a given string or ontology code.

        Args:
            query: 
        If you have a code:
            if you have an OMIMid, use omim_lookup to find one of our curated mapping to EFO
            if you have an ICD9id, find a xref to EFO from oxo
        
        however, if you don't have a code and just a string:
            1. search exact match to name in EFO (or synonyms)
            2. (search fuzzy match to name in EFO)
            3. search within our open targets zooma mappings
            3. search in OLS
            4. search in Zooma High confidence set
        
        '''
        if code:
            try:
                return make_uri(self._find_efo_from_code(query, code=code))
            except Exception as e:
                logger.error(e)
                return None
        else:
            return make_uri(self._find_efo_from_string(query))


    def _find_efo_from_code(self, query, code):
        #FIXME need to properly deal with NotFound scenario
        if code == 'OMIM':
            return self.omim_lookup(query)
        if code == 'ICD9CM':
            return self.icd9_lookup(query)
        logger.warning('Could not find EFO for ID: {} in {}'.format(query, code))
        return None
    

    def _find_efo_from_string(self, query):
        '''
        operations roughly ordered from least expensive to most expensive
        and also from most authorative to least authorative
        '''
        if self.efo_lookup(query):
            return self.efo_lookup(query)
        if self.otzooma_map_lookup(query):
            return self.otzooma_map_lookup(query)
        if self.ols_lookup(query):
            return self.ols_lookup(query)
        if self.zooma_lookup(query):
            return self.zooma_lookup(query)
        logger.warning('Could not find EFO for string: {}'.format(query))
        return None

