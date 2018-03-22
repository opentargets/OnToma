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



def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    ... seealso: https://stevenloria.com/lazy-properties/
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property


def name_to_label_mapping(obonetwork):
    '''
    builds name <=> label lookup dictionaries starting
    from an OBO file, including synonyms
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
                             The function won't break if a valid URI is passed.

    Returns:
        A full URI. Raises a `ValueError` if the short form code was not 
        one of the supported ones.

    Example:
        >>> make_uri('EFO:0000270')
        'http://www.ebi.ac.uk/efo/EFO_0000270'
        
        >>> make_uri('HP_0000270')
        'http://purl.obolibrary.org/obo/HP_0000270'
        
        >>> make_uri('http://purl.obolibrary.org/obo/HP_0000270')
        'http://purl.obolibrary.org/obo/HP_0000270'

        >>> make_uri('TEST_0000270')
        Traceback (most recent call last):
            ...
        ValueError: Could not build an URI. Short form: TEST_0000270 not recognized
    '''
    if ontology_short_form is None: 
        return 
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
        raise ValueError("Could not build an URI. "
                         "Short form: {} not recognized".format(ontology_code))


class OnToma(object):
    '''Open Targets ontology mapping wrapper

    The output should always be a EFO/OpenTargets ontology URI.

    Example:
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

        It returns `None` if it cannot find an EFO id:
        
        >>> t.find_efo('notadisease') is None
        True


    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        '''Initialize API clients'''
        self._ols = OlsClient(ontology=['efo'],field_list=['iri'])
        self._zooma = ZoomaClient()
        self._oxo = OxoClient()
        '''Load OT specific mappings from our github repos'''
        self._zooma_to_efo_map = get_ot_zooma_to_efo_mappings(URLS.ZOOMA_EFO_MAP)
        self._omim_to_efo = get_omim_to_efo_mappings(URLS.OMIM_EFO_MAP)
        

    @lazy_property
    def _efo(self, efourl = URLS.EFO):
        '''Parse the EFO obo file for exact match lookup'''
        _efo = obonet.read_obo(efourl)
        self.logger.info('EFO OBO parsed. Size: {} nodes'.format(len(_efo)))
        return _efo

    @lazy_property
    def _hp(self, hpurl = URLS.HP):
        '''Parse the HP obo file for exact match lookup'''
        _hp = obonet.read_obo(hpurl)
        self.logger.info('HP OBO parsed. Size: {} nodes'.format(len(_hp)))
        return _hp
   
    @lazy_property
    def _icd9_to_efo(self):
        _icd9_to_efo = self._oxo.make_mappings(input_source = "ICD9CM",
                                                   mapping_target= 'EFO')
        return _icd9_to_efo

    @lazy_property
    def efo_to_name(self):
        '''Create name <=> label mappings'''
        efo_to_name, _ = name_to_label_mapping(self._efo)
        return efo_to_name
    
    @lazy_property
    def name_to_efo(self):
        '''Create name <=> label mappings'''
        _, name_to_efo = name_to_label_mapping(self._efo)
        return name_to_efo
    
    @lazy_property
    def hp_to_name(self):
        '''Create name <=> label mappings'''
        hp_to_name, _ = name_to_label_mapping(self._hp)
        return hp_to_name
    
    @lazy_property
    def name_to_hp(self):
        '''Create name <=> label mappings'''
        _, name_to_hp = name_to_label_mapping(self._hp)
        return name_to_hp



    def get_efo_label(self, efocode):
        '''Given an EFO code, returns name/label 
        
        Currently based on the EFO OBO file alone.
        '''
        try:
            return self.efo_to_name[efocode.replace('_',':')]
        except KeyError:
            logger.error('EFO ID {} not found'.format(efocode))
            return None

    def zooma_lookup(self, name):
        '''Searches against the EBI Zooma service for an high confidence mapping
        '''
        return self._zooma.besthit(name)

    def otzooma_map_lookup(self, name):
        '''Searches against the curated OpenTargets mapping we submitted to zooma.
        
        These mappings are usually stored on github.
        NOTE: this is not a lookup to zooma API 
        '''
        return self._zooma_to_efo_map[name]

    def icd9_lookup(self, icd9code):
        '''Searches the ICD9CM <=> EFO mappings returned from the OXO API
        '''
        return self._icd9_to_efo[icd9code]

    def omim_lookup(self, omimcode):
        '''Searches our own curated OMIM <=> EFO mappings
        #FIXME assumes the first is the best hit. is this ok?
        '''
        return self._omim_to_efo[omimcode][0]
    
    def ols_lookup(self, name):
        '''Searches the EBI OLS API for a best match from the EFO
        '''
        s = self._ols.besthit(name)
        if s:
            return s['iri']
        else:
            return None

    def hp_lookup(self, name):
        '''Searches the HP OBO file for a direct match
        '''
        return self.name_to_hp[name]
    
    def efo_lookup(self, name):
        '''Searches the EFO OBO file for a direct match
        '''
        return self.name_to_efo[name]

    def oxo_lookup(self, other_ontology_id, input_source="ICD9CM"):
        '''Searches in the mappings returned from the EBI OXO API. 

        The function should return an EFO code for any given xref, if one
        exists.

        Args: 
            other_ontology_id: the code that should be mapped to EFO
            input_source: an ontology code. Defaults to 'ICD9CM'.
                Available ontologies are listed at https://www.ebi.ac.uk/spot/oxo/api/datasources?fields=preferredPrefix
        
        Returns:
            str: the EFO code
        '''
        return self._oxo.search(ids=[other_ontology_id],
                                input_source=input_source,
                                mapping_target = 'EFO', 
                                distance = 2)


    def find_efo(self, query, code=None):
        '''Finds the most likely EFO code for a given string or ontology code.

        If the code argument is passed, it will attempt to perform an exact match
        amongst the mappings available. 

        If only a string is passed, it will attempt to match it against mappings,
        but will try using the EBI SPOT APIs if no match is found, until a likely
        code is identified

        **TODO** suggestions and fuzzy search should be returned. A specific 
        exception should be crafted and handled. 

        Args:
            query (str): the disease/phenotype to be matched to an EFO code
            code: accepts one of "ICD9CM", "OMIM"
                **TODO** expand to more ontologies
                If a code is passed, it will attempt to find the code in one of 
                our curated mapping datasources. Defaults to None.
        
        Returns:
            A valid OT ontology URI. `None` if no EFO code was found        
        '''

        if code:
            try:
                uri = make_uri(self._find_efo_from_code(query, code=code))
                logger.info('Found {} for {} in {} '
                             'mappings'.format(uri,query,code))
                return uri
            except KeyError as ke:
                logger.info('Could not find a match '
                             'for {} in {} mappings. '.format(ke,code))             
                return None
        else:
            found, source = self._find_efo_from_string(query)
            if found:
                logger.info('Found {} for {} '
                             'from {}'.format(make_uri(found),query,source))
                return make_uri(found)
            else:
                logger.error('Could not find *any* EFO for string: {}'.format(query))
                return

    def _find_efo_from_code(self, query, code):
        '''Finds EFO code given another ontology code

        Returns: `None` if no EFO code was found by this method
        '''
        if code == 'OMIM':
            return self.omim_lookup(query)
        if code == 'ICD9CM':
            return self.icd9_lookup(query)

        logger.error('Code {} is not currently supported.'.format(code))
        return None
    

    def _find_efo_from_string(self, query):
        '''Searches for a matching EFO code for a given phenotype/disease string

        operations roughly ordered from least expensive to most expensive
        and also from most authorative to least authorative

        1. search exact match to name in EFO (or synonyms)
        2. (search fuzzy match to name in EFO)
        3. search within our open targets zooma mappings
        3. search in OLS
        4. search in Zooma High confidence set

        '''

        try:
            return (self.efo_lookup(query),'EFO OBO')
        except KeyError as e:
            logger.debug('Failed EFO OBO lookup for {}'.format(e))
        
        try:
            return (self.otzooma_map_lookup(query), 'OT Zooma Mappings')
        except KeyError as e:
            logger.debug('Failed Zooma Mappings lookup for {}'.format(e))
        
        if self.zooma_lookup(query):
            return (self.zooma_lookup(query), 'Zooma API lookup')
        else:
            logger.debug('Failed Zooma API lookup for {}'.format(query))

        if self.ols_lookup(query):
            return (self.ols_lookup(query), 'OLS API lookup')
        else:
            logger.debug('Failed OLS API lookup for {}'.format(query)) 

        try:
            hpterm = self.hp_lookup(query)
            logger.error('Using the HP term: {} Please check if it is '
                         'actually contained in the Open '
                         'Targets ontology.'.format(hpterm))
            return (hpterm, 'HPO OBO lookup')
        except KeyError as e:
            logger.debug('Failed HP OBO lookup for {}'.format(e))


        #if everything else fails:
        return (None, None)

