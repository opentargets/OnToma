__all__ = ["get_answer","get_opentargets_zooma_to_efo_mappings"]

import logging
logger = logging.getLogger(__name__)


    def search_request_to_ols(self, query=None, ontology='efo', type='class'):
        in_ols = False
        if query in self.ols_synonyms:
            return True
        elif query in self.not_ols_synonyms:
            return False
        else:
            # curl 'http://www.ebi.ac.uk/ols/api/search?q=Myofascial%20Pain%20Syndromes&queryFields=synonym&exact=true&ontology=efo,ordo' -i -H 'Accept: application/json'
            url = 'http://www.ebi.ac.uk/ols/api/search?q=%s&queryFields=synonym&exact=true&ontology=efo,ordo,hpo'%(query)
            self._logger.info("Requesting '%s' as synonym to OLS"%(query))
            print("Requesting '%s' as synonym to OLS" %(query))
            r = requests.get(
                'http://www.ebi.ac.uk/ols/api/search',
                params={'q': query, 'queryFields': 'synonym', 'exact': 'true', 'ontology': 'efo,ordo,hpo'})
            print(r.text)
            results = r.json()
            if results["response"]["numFound"] > 0:
                in_ols = True
                docs = results["response"]["docs"]
                for doc in docs:
                    if doc["ontology_name"] == 'efo':
                        self.ols_synonyms[query] = { "iri" : doc["iri"], "label" : doc["label"], "ontology_name" : doc["ontology_name"] }
                        return True
                for doc in docs:
                    if doc["ontology_name"] == 'hpo':
                        self.ols_synonyms[query] = { "iri" : doc["iri"], "label" : doc["label"], "ontology_name" : doc["ontology_name"] }
                        return True
                for doc in docs:
                    if doc["ontology_name"] == 'ordo':
                        self.ols_synonyms[query] = { "iri" : doc["iri"], "label" : doc["label"], "ontology_name" : doc["ontology_name"] }
                        return True
            else:
                self.not_ols_synonyms.add(query)
        return in_ols


def use_zooma(self):
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
