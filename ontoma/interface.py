# -*- coding: utf-8 -*-
__all__ = ["OnToma"]

from ontoma.downloaders import get_opentargets_zooma_to_efo_mappings
from ontoma.query import ols

from ontoma import URLS

import requests
import csv
import json
from io import BytesIO, TextIOWrapper
import obonet
import python_jsonschema_objects as pjs 


import logging
logger = logging.getLogger(__name__)

# if you want to use across classes
# _cache = {"files":None}


class OnToma(object):
    '''Open Targets ontology mapping cascade

    if you have an id, find a xref to EFO
    elif search exact match to name in EFO (or synonyms)
    elif search fuzzy match to name in EFO
    elif search in OLS
    elif search in Zooma High confidence set

    Initialize the class (which will download EFO,OBO and others):
    >>> t=OnToma()

    We can now lookup "asthma" and get:
    >>> t.efo_lookup('asthma')
    'EFO:0000270'

    We can now lookup "Phenotypic abnormality" on HP: 
    >>> t.hp_lookup('Phenotypic abnormality')
    'HP:0000118'

    Searching the ICD9 code for 'other dermatoses' returns EFO's skin disease:
    >>> t.oxo_lookup('702')
    'EFO:0000701'
    '''

    def __init__(self, efourl = URLS.EFO, 
                        hpurl = URLS.HP):

        self.logger = logging.getLogger(__name__)

        '''Parse the ontology obo files for exact match lookup'''
        self.efo = obonet.read_obo(efourl)
        self.logger.info('EFO parsed. Size: {} nodes'.format(len(self.efo)))
        self.hp = obonet.read_obo(hpurl)
        self.logger.info('HP parsed. Size: {} nodes'.format(len(self.hp)))

        '''Create name mappings'''

        # id_to_name = {id_: data['name'] for id_, data in efo.nodes(data=True)}
        self.name_to_efo = {data['name']: id_ 
                            for id_, data in self.efo.nodes(data=True)}
        
        self.name_to_hp = {data['name']: id_ 
                           for id_, data in self.hp.nodes(data=True)}

        # self.zooma = get_opentargets_zooma_to_efo_mappings()

        # self.icd9_to_efo = {}
        # payload={"ids":[],"inputSource":"ICD9CM","mappingTarget":["EFO"],"distance":"3"}
        # r = requests.post('https://www.ebi.ac.uk/spot/oxo/api/search?size=1000', data= payload)
        # oxomappings = r.json()['_embedded']['searchResults']
        #     while oxo.json().get('next') == True:
        # for row in oxomappings:
        #     self.icd9_to_efo[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']

        # self.omim_to_efo_map = OrderedDict()
        # self.zooma_to_efo_map = OrderedDict()

    def get_omim_to_efo_mappings(self):
        self.logger.debug("OMIM to EFO parsing - requesting from URL %s" % URLS.OMIM_EFO_MAP)
        response = urllib.request.urlopen(URLS.OMIM_EFO_MAP)
        self.logger.info("OMIM to EFO parsing - response code %s" % response.status)
        line_count = 0
        for line in response.readlines():
            '''
            omim	efo_uri	efo_label	source	status
            '''
            line_count += 1
            (omim, efo_uri, efo_label, source, status) = line.decode('utf8').strip().split("\t")
            if omim not in self.omim_to_efo_map:
                self.omim_to_efo_map[omim] = []
            self.omim_to_efo_map[omim].append({'efo_uri': efo_uri, 'efo_label': efo_label})
        return line_count

    






    def hp_lookup(self, name):
        return self.name_to_hp[name]
    
    def efo_lookup(self, name):
        return self.name_to_efo[name]

    def oxo_lookup(self, other_ontology_id):
        '''should return an EFO code for any given xref'''
        return None
