
# -*- coding: utf-8 -*-

import json
import os

__all__ = [
    'URLS',
]



class URLS:
    '''constants for urls for ontology files, mapping and API'''

    OLS = 'http://www.ebi.ac.uk/ols'
    ZOOMA = 'https://www.ebi.ac.uk/spot/zooma/v2/api'

    EFO = 'https://github.com/EBISPOT/efo/raw/v2018-01-15/efo.obo'
    HP = 'http://purl.obolibrary.org/obo/hp.obo'

    OMIM_EFO_MAP = 'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/xref_mappings/omim_to_efo.txt'
    ZOOMA_EFO_MAP = 'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/zooma/cttv_indications_3.txt'