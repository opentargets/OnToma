__all__ = [
    "get_opentargets_zooma_to_efo_mappings",
    "get_omim_to_efo_mappings"
    ]

import csv
import requests
import logging
logger = logging.getLogger(__name__)

def get_omim_to_efo_mappings(url):
    '''returns a dictionary that maps OMIM codes to EFO_uri
    >>> d = get_omim_to_efo_mappings('https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/xref_mappings/omim_to_efo.txt')
    >>> d['609909']
    ['http://www.orpha.net/ORDO/Orphanet_154', 'http://www.orpha.net/ORDO/Orphanet_217607']
    '''
    mappings = {}
    logger.debug("OMIM to EFO mappings - requesting from URL {}".format(url))
    with requests.get(url, stream=True) as r:
        for i,row in enumerate(csv.DictReader(r.iter_lines(),delimiter='\t')):
            if row['OMIM'] not in mappings:
                mappings[row['OMIM']] = []
            mappings[row['OMIM']].append(row['efo_uri'])
    logger.debug("Parsed {} rows".format(i))
    return mappings



# payload={"ids":[],"inputSource":"ICD9CM","mappingTarget":["EFO"],"distance":"3"}
# r = requests.post('https://www.ebi.ac.uk/spot/oxo/api/search?size=1000', data= payload)
# oxomappings = r.json()['_embedded']['searchResults']
#     while oxo.json().get('next') == True:
# for row in oxomappings:
#     self.icd9_to_efo[ row['curie'].split(':')[1] ] = row['mappingResponseList'][0]['curie']


def use_zooma():
        '''
        Call request to Zooma function
        :return: None.
        '''


        logger.info("use Zooma")
        for phenotype in self.phenotype_set:
            if phenotype:
                self._logger.info("Mapping '%s' with zooma..."%(phenotype))
                self.request_to_zooma(phenotype)

        with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
            tsv_writer = csv.writer(outfile, delimiter='\t')
            for phenotype, value in self.high_confidence_mappings.items():
                tsv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

        with open(Config.GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT, 'w') as map_file:
            csv_writer = csv.writer(map_file, delimiter='\t')
            for phenotype, value in self.other_zooma_mappings.items():
                csv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])


def get_opentargets_zooma_to_efo_mappings():
    pass
#     '''download zooma and returns a dict
#     >>> d = get_opentargets_zooma_to_efo_mappings()
#     >>> d['bla']
#     KeyError
#     '''
#     zooma_to_efo_map = {}
#     logger.info("ZOOMA to EFO parsing - requesting from URL %s" % Config.ZOOMA_TO_EFO_MAP_URL)
#     response = urllib.request.urlopen(Config.ZOOMA_TO_EFO_MAP_URL)
#     logger.info("ZOOMA to EFO parsing - response code %s" % response.status)
#     n = 0
#     for n, line in enumerate(response):
#         '''
#         STUDY	BIOENTITY	PROPERTY_TYPE	PROPERTY_VALUE	SEMANTIC_TAG	ANNOTATOR	ANNOTATION_DATE
#         disease	Amyotrophic lateral sclerosis 1	http://www.ebi.ac.uk/efo/EFO_0000253
#         '''
#         if n == 0: continue

#         #logger.info("[%s]"%line)
#         (study, bioentity, property_type, property_value, semantic_tag, annotator, annotation_date) = line.decode('utf8').strip().split("\t")
#         if property_value.lower() not in self.omim_to_efo_map:
#             zooma_to_efo_map[property_value.lower()] = []
#         zooma_to_efo_map[property_value.lower()].append({'efo_uri': semantic_tag, 'efo_label': semantic_tag})
    
#     return zooma_to_efo_map

