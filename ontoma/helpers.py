__all__ = ["get_answer","get_opentargets_zooma_to_efo_mappings"]

import logging
logger = logging.getLogger(__name__)

def get_opentargets_zooma_to_efo_mappings(self):
    '''download zooma and returns a dict
    >>> d = get_opentargets_zooma_to_efo_mappings()
    >>> d['bla']
    KeyError
    '''
    zooma_to_efo_map = {}
    logger.info("ZOOMA to EFO parsing - requesting from URL %s" % Config.ZOOMA_TO_EFO_MAP_URL)
    response = urllib.request.urlopen(Config.ZOOMA_TO_EFO_MAP_URL)
    logger.info("ZOOMA to EFO parsing - response code %s" % response.status)
    n = 0
    for n, line in enumerate(response):
        '''
        STUDY	BIOENTITY	PROPERTY_TYPE	PROPERTY_VALUE	SEMANTIC_TAG	ANNOTATOR	ANNOTATION_DATE
        disease	Amyotrophic lateral sclerosis 1	http://www.ebi.ac.uk/efo/EFO_0000253
        '''
        if n == 0: continue

        #logger.info("[%s]"%line)
        (study, bioentity, property_type, property_value, semantic_tag, annotator, annotation_date) = line.decode('utf8').strip().split("\t")
        if property_value.lower() not in self.omim_to_efo_map:
            zooma_to_efo_map[property_value.lower()] = []
        zooma_to_efo_map[property_value.lower()].append({'efo_uri': semantic_tag, 'efo_label': semantic_tag})
    
    return zooma_to_efo_map



def get_answer():
    """Get an answer."""
    return True
