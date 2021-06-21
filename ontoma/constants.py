"""Constants for urls for ontology files, mapping and API."""

URLS = {
    'EFO': 'https://www.ebi.ac.uk/efo/efo.obo',
    'HP': 'http://purl.obolibrary.org/obo/hp.obo',
    'MANUAL_XREF': 'https://raw.githubusercontent.com/opentargets/mappings/master/manual_xref.tsv',
    'ZOOMA_EFO_MAP': 'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/zooma/cttv_indications_3.txt',
    'MONDO': 'http://purl.obolibrary.org/obo/mondo.obo'
}

OT_TOP_NODES = {
    'http://www.ebi.ac.uk/efo/EFO_0005932',          # Animal disease
    'http://purl.obolibrary.org/obo/GO_0008150',     # Biological process
    'http://purl.obolibrary.org/obo/MONDO_0045024',  # Cell proliferation disorder
    'http://purl.obolibrary.org/obo/MONDO_0021205',  # Disease of ear
    'http://purl.obolibrary.org/obo/MONDO_0024458',  # Disease of visual system
    'http://www.ebi.ac.uk/efo/EFO_0001379',          # Endocrine system disease
    'http://www.ebi.ac.uk/efo/EFO_0010282',          # Gastrointestinal disease
    'http://www.ebi.ac.uk/efo/OTAR_0000018',         # Genetic, familial or congenital disease
    'http://www.ebi.ac.uk/efo/EFO_0000508',          # Genetic disorder (child of OTAR_0000018)
    'http://www.ebi.ac.uk/efo/OTAR_0000019',         # Familial disease (child of OTAR_0000018; has no children)
    'http://www.ebi.ac.uk/efo/MONDO_0018797',        # Genetic cardiac malformation (child of OTAR_0000018)
    'http://www.ebi.ac.uk/efo/MONDO_0000839',        # Congenital abnormality (child of OTAR_0000018)
    'http://www.ebi.ac.uk/efo/EFO_0000319',          # Cardiovascular disease
    'http://www.ebi.ac.uk/efo/EFO_0005803',          # Hematologic disease
    'http://www.ebi.ac.uk/efo/EFO_0000540',          # Immune system disease
    'http://www.ebi.ac.uk/efo/EFO_0005741',          # Infectious disease
    'http://www.ebi.ac.uk/efo/OTAR_0000009',         # Injury, poisoning or other complication
    'http://www.ebi.ac.uk/efo/EFO_0008546',          # Poisoning (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_1000903',          # Drug-induced akathisia (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_1000904',          # Drug-Induced dyskinesia (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/MONDO_0001423',        # Drug-induced mental disorder (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0004228',          # Drug-induced liver injury (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0009482',          # Drug allergy  (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0009518',          # Complication (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0000546',          # Injury (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0003099',          # Cushing syndrome (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/MONDO_0016474',        # Drug-induced lupus erythematosus (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0005400',          # chemotherapy-induced alopecia (child of OTAR_0000009)
    'http://www.ebi.ac.uk/efo/EFO_0010285',          # Integumentary system disease
    'http://www.ebi.ac.uk/efo/EFO_0001444',          # Measurement
    'http://www.ebi.ac.uk/efo/OTAR_0000006',         # Musculoskeletal or connective tissue disease
    'http://www.ebi.ac.uk/efo/EFO_1001986',          # Connective tissue disease (child of OTAR_0000006)
    'http://www.ebi.ac.uk/efo/EFO_0009676',          # Musculoskeletal system disease (child of OTAR_0000006)
    'http://www.ebi.ac.uk/efo/EFO_0000618',          # Nervous system disease
    'http://purl.obolibrary.org/obo/MONDO_0024297',  # Nutritional or metabolic disease
    'http://www.ebi.ac.uk/efo/EFO_0009605',          # Pancreas disease
    'http://www.ebi.ac.uk/efo/EFO_0000651',          # Phenotype
    'http://www.ebi.ac.uk/efo/OTAR_0000014',         # Pregnancy or perinatal disease
    'http://www.ebi.ac.uk/efo/EFO_0009683',          # Puerperal disorder (child of OTAR_0000014)
    'http://www.ebi.ac.uk/efo/EFO_0009682',          # Pregnancy disorder (child of OTAR_0000014)
    'http://www.ebi.ac.uk/efo/EFO_0010238',          # Perinatal disease (child of OTAR_0000014)
    'http://purl.obolibrary.org/obo/MONDO_0002025',  # Psychiatric disorder
    'http://www.ebi.ac.uk/efo/OTAR_0000017',         # Reproductive system or breast disease
    'http://www.ebi.ac.uk/efo/EFO_0000512',          # Reproductive system disease (child of OTAR_0000017)
    'http://www.ebi.ac.uk/efo/EFO_0009483',          # Breast disease (child of OTAR_0000017)
    'http://www.ebi.ac.uk/efo/OTAR_0000010',         # Respiratory or thoracic disease
    'http://www.ebi.ac.uk/efo/EFO_0000684',          # Respiratory system disease (child of OTAR_0000010)
    'http://www.ebi.ac.uk/efo/MONDO_0000651',        # Thoracic disease (child of OTAR_0000010)
    'http://www.ebi.ac.uk/efo/EFO_0009690',          # Urinary system disease
}

FIELDS = ['query', 'term', 'label', 'source', 'quality', 'action']
