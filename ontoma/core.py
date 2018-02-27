# -*- coding: utf-8 -*-
from . import helpers


import requests
import csv
import json
from io import BytesIO, TextIOWrapper
import obonet
import python_jsonschema_objects as pjs 


import logging
logger = logging.getLogger(__name__)



EFO_URL = 'https://github.com/EBISPOT/efo/raw/v2018-01-15/efo.obo'
HP_URL = 'http://purl.obolibrary.org/obo/hp.obo'

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

    def __init__(self):

        '''Parse the ontology obo files for exact match lookup'''

        self.efo = obonet.read_obo(EFO_URL)
        logger.debug('yeah!')
        logger.info('EFO parsed. Size: {} nodes'.format(len(self.efo)))
        self.hp = obonet.read_obo(HP_URL)
        logger.info('HP parsed. Size: {} nodes'.format(len(self.hp)))

        '''Create name mappings'''

        # id_to_name = {id_: data['name'] for id_, data in efo.nodes(data=True)}
        self.name_to_efo = {data['name']: id_ 
                            for id_, data in self.efo.nodes(data=True)}
        
        self.name_to_hp = {data['name']: id_ 
                           for id_, data in self.hp.nodes(data=True)}

        # self.icd9_to_efo = {}
        # payload={"ids":[],"inputSource":"ICD9CM","mappingTarget":["EFO"],"distance":"3"}
        # r = requests.post('https://www.ebi.ac.uk/spot/oxo/api/search?size=1000', data= payload)
        # oxomappings = r.json()['_embedded']['searchResults']
        #     while oxo.json().get('next') == True:
        # for row in oxomappings:
        #     self.icd9_to_efo[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']




    def hp_lookup(self, name):
        return self.name_to_hp[name]
    
    def efo_lookup(self, name):
        return self.name_to_efo[name]

    def oxo_lookup(self, other_ontology_id):
        '''should return an EFO code for any given xref'''
        return None
