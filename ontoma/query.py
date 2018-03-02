__all__ = ["ols"]

import requests
import logging
logger = logging.getLogger(__name__)

def ols(query=None, ontology='efo', type='class'):
    ''' 
    Launch an API request to the OLS service similar to:
    curl 'http://www.ebi.ac.uk/ols/api/search?q=Myofascial%20Pain%20Syndromes&queryFields=synonym&exact=true&ontology=efo,ordo' -i -H 'Accept: application/json'
    >>> ols('Myofascial Pain Syndrome')
    'EFO_1001054'
    '''
    logger.debug("Requesting '{}' as synonym to OLS".format(query))
    payload={'q': query, 
             'queryFields': 'synonym',
             'exact': 'true', 
             'ontology': 'efo,ordo,hpo'}

    r = requests.get('http://www.ebi.ac.uk/ols/api/search', params=payload)
    r.raise_for_status()
    logger.debug(r.text)
    results = r.json()
    if results["response"]["numFound"] > 0:
        for doc in results["response"]["docs"]:
            if (doc["ontology_name"] == 'efo' or 
                doc["ontology_name"] == 'hpo' or 
                doc["ontology_name"] == 'ordo'):
                self.ols_synonyms[query] = { "iri" : doc["iri"], "label" : doc["label"], "ontology_name" : doc["ontology_name"] }
                return True
        return True
    else:
        logger.warning("{} not in OLS".format(query))
        return False
