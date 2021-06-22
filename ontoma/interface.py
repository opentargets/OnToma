"""Main interface class."""

__all__ = [
    "OnToma",
    "make_uri"
]

from functools import lru_cache
import logging
import os

import obonet
import pandas as pd

from ontoma.downloaders import get_manual_xrefs, get_manual_string_mappings
from ontoma.ols import OlsClient
from ontoma.zooma import ZoomaClient
from ontoma.oxo import OxoClient
from ontoma.constants import URLS, OT_TOP_NODES
from ontoma import ontology
from ontoma import owl


logger = logging.getLogger(__name__)


def lazy_property(fn):
    """Decorator that makes a property lazy-evaluated.
    ... seealso: https://stevenloria.com/lazy-properties/"""
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property


def name_to_label_mapping(obonetwork):
    """
    builds name <=> label lookup dictionaries starting
    from an OBO file, including synonyms
    """
    id_to_name = {}
    name_to_id = {}
    for nodeid, data in obonetwork.nodes(data=True):
        try:
            id_to_name[nodeid] = data['name']
            name_to_id[data['name']] = nodeid
        except KeyError:
            # skip the nodes that don't have data
            # (eg. https://rarediseases.info.nih.gov/diseases/4781/seckel-like-syndrome-majoor-krakauer-type in MONDO)
            pass

        if 'synonym' in data:
            for synonim in data['synonym']:
                #some str.split voodoo is necessary to unpact the synonyms
                # as specified in the obo file
                name_to_id[synonim.split('\"')[1]] = nodeid
    return id_to_name, name_to_id


def xref_to_name_and_label_mapping(obonetwork):
    '''
    builds xref to list of name and label lookup dictionary starting from an OBO file
    '''
    xref_to_name_and_label = {}
    for nodeid, data in obonetwork.nodes(data=True):
        if 'xref' in data:
            for xref in data['xref']:
                name_and_label = {'id': nodeid, 'name': data['name']}
                if xref in xref_to_name_and_label:
                    xref_to_name_and_label[xref].append(name_and_label)
                else:
                    xref_to_name_and_label[xref] = [name_and_label]
    return xref_to_name_and_label


def make_uri(ontology_short_form):
    '''
    Transform a short form ontology code in a full URI.
    Currently works for EFO, HPO, MONDO, UBERON, ORDO
    GO, NCIT, DOID, and MP.

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
        return None
    if ontology_short_form.startswith('http'):
        return ontology_short_form
    ontology_code = ontology_short_form.replace(':', "_")
    if ontology_code.startswith('EFO'):
        return 'http://www.ebi.ac.uk/efo/'+ontology_code
    elif (ontology_code.startswith('HP') or
          ontology_code.startswith('MP') or
          ontology_code.startswith('MONDO') or
          ontology_code.startswith('UBERON') or
          ontology_code.startswith('GO') or
          ontology_code.startswith('NCIT') or
          ontology_code.startswith('DOID')):
        return 'http://purl.obolibrary.org/obo/' + ontology_code
    elif ontology_code.startswith('Orphanet'):
        return 'http://www.orpha.net/ORDO/' + ontology_code
    else:
        raise ValueError("Could not build an URI. "
                         "Short form: {} not recognized".format(ontology_code))


class OnToma(object):
    """Open Targets ontology mapping wrapper

    Args:
        exclude (str or [str]):  Excludes 'zooma','ols_hp' or 'ols_ordo' API
                                 calls from the search, to speed things up.


    Example:
        Initialize the class (which will download EFO,OBO and others):

        >>> t=OnToma()

        We can now lookup "asthma" and get:

        >>> t.efo_lookup('asthma')
        'http://www.ebi.ac.uk/efo/EFO_0000270'

        Notice we always tend to return a full IRI

        Search by synonyms coming from the OBO file is also supported

        >>> t.efo_lookup('Asthma unspecified')
        'http://www.ebi.ac.uk/efo/EFO_0000270'

        Reverse lookups uses the get_efo_label() method

        >>> t.get_efo_label('EFO_0000270')
        'asthma'
        >>> t.get_efo_label('EFO:0000270')
        'asthma'
        >>> t.get_efo_label('http://www.ebi.ac.uk/efo/EFO_0000270')
        'asthma'


        Similarly, we can now lookup "Phenotypic abnormality" on HP OBO:

        >>> t.hp_lookup('Phenotypic abnormality')
        'http://purl.obolibrary.org/obo/HP_0000118'
        >>> t.hp_lookup('Narrow nasal tip')
        'http://purl.obolibrary.org/obo/HP_0011832'

        OMIM code lookup

        >>> t.omim_lookup('230650')
        'http://www.orpha.net/ORDO/Orphanet_79257'

        >>> t.zooma_lookup('asthma')
        'http://www.ebi.ac.uk/efo/EFO_0000270'

        MONDO lookup
        >>> t.mondo_lookup('asthma')
        'http://purl.obolibrary.org/obo/MONDO_0004979'

        There is also a semi-intelligent wrapper, which tries to guess the
        best matching strategy:

        >>> t.find_term('asthma')
        'http://www.ebi.ac.uk/efo/EFO_0000270'
        >>> t.find_term('615877',code='OMIM')
        'http://www.orpha.net/ORDO/Orphanet_202948'

        It returns `None` if it cannot find an EFO id:

        >>> t.find_term('notadisease') is None
        True

    """

    def __init__(self, cache_dir, exclude=[]):
        self.logger = logging.getLogger(__name__)
        self.exclude = exclude

        # Initialize API clients.
        self._oxo = OxoClient()
        self._zooma = ZoomaClient()
        self._ols = OlsClient()

        # Import EFO OWL datasets.
        self.efo_terms = pd.read_csv(os.path.join(cache_dir, owl.TERMS_FILENAME), sep='\t')
        self.efo_xrefs = pd.read_csv(os.path.join(cache_dir, owl.XREFS_FILENAME), sep='\t')
        self.efo_synonyms = pd.read_csv(os.path.join(cache_dir, owl.SYNONYMS_FILENAME), sep='\t')
        self.logger.info(f'Loaded {len(self.efo_terms)} terms, {len(self.efo_xrefs)} xrefs, '
                         f'and {len(self.efo_synonyms)} synonyms from EFO OWL.')

        # Import manually curated datasets.
        self.manual_xrefs = get_manual_xrefs(URLS['MANUAL_XREF'])
        self.manual_string = get_manual_string_mappings(URLS['MANUAL_STRING'])

    @lazy_property
    def _efo(self, efourl=URLS['EFO']):
        '''Parse the EFO obo file for exact match lookup'''
        _efo = obonet.read_obo(efourl)
        self.logger.info('EFO OBO parsed. Size: %s nodes', len(_efo))
        return _efo

    @lazy_property
    def _mondo(self, mondourl=URLS['MONDO']):
        '''Parse the EFO obo file for exact match lookup'''
        _mondo = obonet.read_obo(mondourl)
        self.logger.info('MONDO OBO parsed. Size: %s nodes', len(_mondo))
        return _mondo

    @lazy_property
    def _hp(self, hpurl=URLS['HP']):
        '''Parse the HP obo file for exact match lookup'''
        _hp = obonet.read_obo(hpurl)
        self.logger.info('HP OBO parsed. Size: %s nodes', len(_hp))
        return _hp

    @lazy_property
    def efo_to_name(self):
        '''Create name <=> label mappings'''
        efo_to_name, _ = name_to_label_mapping(self._efo)
        return efo_to_name

    @lazy_property
    def name_to_efo(self):
        '''Create name <=> label mappings'''
        _, name_to_efo = name_to_label_mapping(self._efo)
        logger.info("Parsed %s Name to EFO mapping " % len(name_to_efo))
        return name_to_efo

    @lazy_property
    def xref_to_efo(self):
        '''Create xref => EFO id and label mappings'''
        xref_to_efo = xref_to_name_and_label_mapping(self._efo)
        return xref_to_efo


    @lazy_property
    def mondo_to_name(self):
        '''Create name <=> label mappings'''
        mondo_to_name, _ = name_to_label_mapping(self._mondo)
        return mondo_to_name

    @lazy_property
    def name_to_mondo(self):
        '''Create name <=> label mappings'''
        _, name_to_mondo = name_to_label_mapping(self._mondo)
        logger.info("Parsed %s Name to mondo mapping " % len(name_to_mondo))
        return name_to_mondo

    @lazy_property
    def hp_to_name(self):
        '''Create name <=> label mappings'''
        hp_to_name, _ = name_to_label_mapping(self._hp)
        return hp_to_name

    @lazy_property
    def name_to_hp(self):
        '''Create name <=> label mappings'''
        _, name_to_hp = name_to_label_mapping(self._hp)
        logger.info("Parsed %s Name to HP mapping " % len(name_to_hp))
        return name_to_hp

    def get_efo_label(self, efocode):
        '''Given an EFO short form code, returns the label as taken from the OBO
        '''
        if '/' in efocode:
            #extract short form from IRI
            efocode = efocode.split('/')[-1]
        try:
            return self.efo_to_name[efocode.replace('_', ':')]
        except KeyError:
            logger.error('EFO ID %s not found', efocode)
            return None

    def get_mondo_label(self, mondocode):
        '''Given an MONDO short form code, returns the label as taken from the OBO
        '''
        if '/' in mondocode:
            #extract short form from IRI
            mondocode = mondocode.split('/')[-1]
        try:
            return self.mondo_to_name[mondocode.replace('_', ':')]
        except KeyError:
            logger.error('MONDO ID %s not found', mondocode)
            return None

    def get_hp_label(self, hpcode):
        '''Given an HP short form code, returns the label as taken from the OBO
        '''
        if '/' in hpcode:
            #extract short form from IRI
            hpcode = hpcode.split('/')[-1]
        try:
            return self.hp_to_name[hpcode.replace('_', ':')]
        except KeyError:
            logger.error('HP ID %s not found', hpcode)
            return None

    def get_efo_from_xref(self, efocode):
        '''Given an short disease id, returns the id and label of equivalent term(s) in EFO as defined by xrefs
        '''
        if '/' in efocode:
            #extract short form from IRI
            efocode = efocode.split('/')[-1]
        try:
            return self.xref_to_efo[efocode.replace('_', ':')]
        except KeyError:
            logger.error('There are no EFO ID that have xrefs to %s', efocode)
            return None

    def zooma_lookup(self, name):
        '''Searches against the EBI Zooma service for an high confidence mapping
        '''
        return self._zooma.besthit(name)['iri']

    def otzooma_map_lookup(self, name):
        '''Searches against the curated OpenTargets mapping we submitted to zooma.

        These mappings are usually stored on github.
        NOTE: this is not a lookup to zooma API
        '''
        return self._zooma_to_efo_map[name]

    def hp_lookup(self, name):
        '''Searches the HP OBO file for a direct match
        '''
        return make_uri(self.name_to_hp[name])

    def efo_lookup(self, name):
        '''Searches the EFO OBO file for a direct match
        '''
        return make_uri(self.name_to_efo[name])

    def mondo_lookup(self, name):
        '''Searches the mondo OBO file for a direct match
        '''
        return make_uri(self.name_to_mondo[name])

    def _is_included(self, iri, ontology=None):
        '''
        FIXME: this function should check against an actual index/ontology,
        not be guessing the right root nodes.

        checks if efo term with given iri has for ancestors one of the nodes
        we select for open targets
        '''
        if not ontology:
            # default to checking ancestry in EFO
            ontology = 'efo'
        try:
            for ancestor in self._ols.get_ancestors(ontology, iri):
                if ancestor['iri'] in OT_TOP_NODES:
                    return True
        except KeyError:
            pass

        return False

    ###############################################################################################################

    def filter_identifiers_by_efo_current(self, normalised_identifiers):
        """Returns a subset of the idenfitiers which are in EFO and not marked as obsolete."""
        return list(
            self.efo_terms[
                (self.efo_terms.normalised_id.isin(normalised_identifiers)) &
                (~ self.efo_terms.is_obsolete)
            ]
            .normalised_id
        )

    def step1_owl_identifier_match(self, normalised_identifier):
        """If the term is already present in EFO, return it as is."""
        return self.filter_identifiers_by_efo_current(
            self.efo_terms[self.efo_terms.normalised_id == normalised_identifier].normalised_id
        )

    def step2_owl_db_xref(self, normalised_identifier):
        """If there are terms in EFO referenced by the `hasDbXref` field to the query, return them."""
        return self.filter_identifiers_by_efo_current(
            self.efo_xrefs[self.efo_xrefs.normalised_xref_id == normalised_identifier].normalised_id
        )

    def step3_manual_xref(self, normalised_identifier):
        """Look for the queried term in the manual ontology-to-ontology mapping list."""
        return self.filter_identifiers_by_efo_current(
            self.manual_xrefs[self.manual_xrefs.normalised_xref_id == normalised_identifier].normalised_id
        )

    def step4_oxo_query(self, normalised_identifier):
        """Find cross-references using OxO."""
        oxo_mappings = set()
        for result in self._oxo.search(ids=[normalised_identifier], mapping_target='EFO', distance=2):
            for mapping in result['mappingResponseList']:
                oxo_mappings.add(ontology.normalise_ontology_identifier(mapping['curie']))
        return self.filter_identifiers_by_efo_current(oxo_mappings)

    def step5_owl_name_match(self, normalised_string):
        """Find EFO terms which match the string query exactly."""
        return self.filter_identifiers_by_efo_current(
            self.efo_terms[self.efo_terms.normalised_label == normalised_string].normalised_id
        )

    def step6_owl_exact_synonym(self, normalised_string):
        """Find EFO terms which have the query as an exact synonym."""
        return self.filter_identifiers_by_efo_current(
            self.efo_synonyms[self.efo_synonyms.normalised_synonym == normalised_string].normalised_id
        )

    def step7_manual_mapping(self, normalised_string):
        """Find the query in the manual string-to-ontology mapping database."""
        return self.filter_identifiers_by_efo_current(
            self.manual_string[self.manual_string.normalised_label == normalised_string].normalised_id
        )

    def step8_zooma_high_confidence(self, normalised_string):
        raise NotImplementedError

    def step9_owl_related_synonym(self, normalised_string):
        raise NotImplementedError

    def step10_zooma_any(self, normalised_string):
        raise NotImplementedError

    def find_term(
            self,
            query: str,
            code: bool = False,
            suggest: bool = False,
            verbose: bool = False,
    ) -> list:
        """For a given query (an ontology identifier or a string), find matches in EFO Open Targets slim.

        The algorithm operates in a series of steps. If a given step is successful, the result is returned immediately,
        and the remaining steps are not executed. If all steps fail to provide a match, None is returned. Note that in
        general more than one mapping can be returned. This can happen for complex traits which require more than one
        ontology term to represent them.

        If the `code` flag is specified, it is assumed that the query is an ontology identifier, such as 'OMIM:615632',
        and the following steps are attempted:
        1. See if the term is already in EFO OT slim OWL file.
        2. Match terms by cross-references (hasDbXref) from the OWL file.
        3. Mapping from the manual cross-reference database.
        4. Request through OxO with a distance of 2.

        If the query is a string, the following steps are attempted:
        5. Exact name match from EFO OT slim OWL file.
        6. Exact synonym (hasExactSynonym) from the OWL file.
        7. Mapping from the manual string-to-ontology database.
        8. High confidence mapping from ZOOMA with default parameters.

        The following functionality is planned, but not yet implemented. — If the query is a string, and additionally
        the `suggest` flag is specified, additional steps are attempted:
        9. Inexact synonyms (hasRelatedSynonym) from the OWL file.
        10. Any confidence mapping from ZOOMA with default parameters.

        Args:
            query: Either an ontology identifier, or the disease/phenotype string to be matched to an EFO code.
            code: Whether to treat the query as an ontology identifier.
            suggest: Whether to report low quality mappings which are not guaranteed to be contained in EFO OT slim.
            verbose: If specified, return a dictionary containing {term, label, source, quality, action} instead of only
                the term.

        Returns:
            A list of values dependent on the `verbose` flag (either strings with ontology identifiers, or a dictionary
            of additional information). The list will be empty if no hits were identified."""

        if verbose:
            raise NotImplementedError

        # Attempt mapping using various strategies for identifier/string inputs.
        if code:
            normalised_identifier = ontology.normalise_ontology_identifier(query)
            result = (
                self.step1_owl_identifier_match(normalised_identifier)
                or self.step2_owl_db_xref(normalised_identifier)
                or self.step3_manual_xref(normalised_identifier)
                or self.step4_oxo_query(normalised_identifier)
            )
        else:
            normalised_string = query.lower()
            result = (
                self.step5_owl_name_match(normalised_string)
                or self.step6_owl_exact_synonym(normalised_string)
                or self.step7_manual_mapping(normalised_string)
                or self.step8_zooma_high_confidence(normalised_string)
            )
            if not result and suggest:
                result = (
                        self.step9_owl_related_synonym(normalised_string) +
                        self.step10_zooma_any(normalised_string)
                )

        # Convert the term representation into the format supported by the Open Targets schema.
        result = [ontology.convert_to_ot_schema(r) for r in result]

        # Return either the list of dictionaries, or just the mappings, depending on parameters.
        logger.info(f'Found: {query} → {result}')
        return result

    @lru_cache(maxsize=None)
    def _find_term_from_string(self, query, suggest=False):
        '''Searches for a matching EFO code for a given phenotype/disease string

        Returns:
            {term, label, source,quality, action}
        '''
        query = query.lower()

        try:
            efolup = self.efo_lookup(query)
            if self._is_included(efolup):
                return {'term': efolup,
                        'label': self.get_efo_label(efolup),
                        'source': 'EFO OBO',
                        'quality': 'match',
                        'action' : None}
        except KeyError as e:
            logger.debug('Failed EFO OBO lookup for %s', e)

        try:
            return {'term': self.otzooma_map_lookup(query),
                    'label': query, #method above only works as an exact match
                    'source': 'OT Zooma Mappings',
                    'quality': 'match',
                    'action' : None}
        except KeyError as e:
            logger.debug('Failed Zooma Mappings lookup for %s', e)

        if 'zooma' not in self.exclude:
            zoomabest = self._zooma.besthit(query)
            if zoomabest and self._is_included(zoomabest['iri']):
                return {'term': zoomabest['iri'],
                        'label': zoomabest['label'],
                        'source': 'Zooma API lookup',
                        'quality': 'match',
                        'action' : None}
            else:
                logger.debug('Failed Zooma API lookup for %s', query)


        exact_ols_efo = self._ols.besthit(query,
                                        ontology=['efo'],
                                        field_list=['iri','label'],
                                        exact=True)
        if exact_ols_efo and self._is_included(exact_ols_efo['iri']):
            return {'term': exact_ols_efo['iri'],
                    'label': exact_ols_efo['label'],
                    'source': 'OLS API EFO lookup',
                    'quality': 'match',
                    'action' : None}
        else:
            logger.debug('Failed OLS API EFO (exact) lookup for %s', query)


        #### below this line mappings should be checked ####

        try:
            hpterm = self.hp_lookup(query)
            logger.warning('Using the HP term: %s for %s Please check if it is '
                         'actually contained in the Open '
                         'Targets ontology.', hpterm, query)
            return {'term': hpterm,
                    'label': self.get_hp_label(hpterm),
                    'source': 'HP OBO lookup',
                    'quality': 'match',
                    'action' : 'check if in OT'}
        except KeyError as e:
            logger.debug('Failed HP OBO lookup for %s', e)

        if 'ols_hp' not in self.exclude:
            exact_ols_hp = self._ols.besthit(query, ontology=['hp'],
                                            field_list=['iri','label'],
                                            exact=True)

            #here I can check if it is the child of the 'disease' HP node we
            # include, though it's probably meaningless to do so.
            # One would still need to include the subtree/nodes in the OT ontology.
            if exact_ols_hp and self._is_included(exact_ols_hp['iri'], ontology='hp'):
                logger.warning('Using the HP term: %s Please check if it is '
                            'actually contained in the Open '
                            'Targets ontology.', exact_ols_hp)
                return {'term': exact_ols_hp['iri'],
                        'label': exact_ols_hp['label'],
                        'source': 'OLS API HP exact lookup',
                        'quality': 'match',
                        'action' : 'check if in OT'}
            else:
                logger.debug('Failed OLS API HP (exact) lookup for %s', query)

        if 'ols_ordo' not in self.exclude:
            exact_ols_ordo = self._ols.besthit(query, ontology=['ordo'],
                                            field_list=['iri','label'],
                                            exact=True)
            if exact_ols_ordo:
                logger.warning('Using the ORDO term: %s Please check if it is '
                            'actually contained in the Open '
                            'Targets ontology.', exact_ols_ordo)
                return {'term': exact_ols_ordo['iri'],
                        'label': exact_ols_ordo['label'],
                        'source': 'OLS API ORDO exact lookup',
                        'quality': 'match',
                        'action' : 'check if in OT'}
            else:
                logger.debug('Failed OLS API ORDO (exact) lookup for %s', query)


        ols_efo = self._ols.besthit(query,
                                    ontology=['efo'],
                                    field_list=['iri','label'],
                                    bytype='class')
        if ols_efo and self._is_included(ols_efo['iri']):
            return {'term': ols_efo['iri'],
                    'label': ols_efo['label'],
                    'source': 'OLS API EFO lookup',
                    'quality': 'fuzzy',
                    'action' : 'check if valid'}
        else:
            logger.debug('Failed OLS API EFO lookup for %s', query)

        if suggest:
            ols_suggestion = self._ols.besthit(query,
                                        ontology=['efo','hp','ordo'],
                                        field_list=['iri','label'],
                                        bytype='class')
            if ols_suggestion:
                logger.warning('Found a fuzzy match in OLS API [EFO,HP,ORDO] - check if valid')
                return {'term': ols_suggestion['iri'],
                        'label': ols_suggestion['label'],
                        'source': 'OLS API [ORDO] lookup',
                        'quality': 'fuzzy',
                        'action' : 'check if valid'}
            else:
                logger.debug('Failed OLS API [EFO,HP,ORDO] lookup for %s', query)

        #if everything else fails:
        return None
