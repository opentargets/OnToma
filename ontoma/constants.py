'''constants for urls for ontology files, mapping and API'''

URLS = {

    'EFO':'https://github.com/EBISPOT/efo/raw/v2018-01-15/efo.obo',
    'HP':'http://purl.obolibrary.org/obo/hp.obo',
    'OMIM_EFO_MAP':'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/xref_mappings/omim_to_efo.txt',
    'ZOOMA_EFO_MAP':'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/zooma/cttv_indications_3.txt',
}
OT_TOP_NODES = {
    'http://www.ebi.ac.uk/efo/EFO_0000408',
    'http://www.ebi.ac.uk/efo/EFO_0000651',
    'http://www.ebi.ac.uk/efo/EFO_0001444',
    'http://purl.obolibrary.org/obo/GO_0008150',
    'http://www.ifomis.org/bfo/1.1/snap#Function',
    'http://www.ebi.ac.uk/efo/EFO_0000546',
    'http://www.ebi.ac.uk/efo/EFO_0003935',
    'http://purl.obolibrary.org/obo/HP_0000118'
}

FIELDS = ['query','term','label','source','quality','action']
