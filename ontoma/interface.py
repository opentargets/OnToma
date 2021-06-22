"""Main interface class."""

__all__ = [
    'OnToma',
]

import logging
import os
import tempfile

import pandas as pd

from ontoma import ontology
from ontoma import owl
from ontoma.constants import URLS
from ontoma.downloaders import get_manual_xrefs, get_manual_string_mappings
from ontoma.oxo import OxoClient
from ontoma.zooma import ZoomaClient

logger = logging.getLogger(__name__)


class OnToma(object):
    """Open Targets ontology mapping wrapper. Please refer to documentation for usage details."""

    def __init__(self, cache_dir=None):
        """Initialise an OnToma instance and fetch the necessary resources. Depending on whether cache_dir is specified
        and whether it contains anything, the following behaviour is applied:
        1. If cache_dir is specified and is not empty, OnToma will use EFO cache from it as is.
        2. If cache_dir is specified and is empty or does not exist, OnToma will download the cache to the directory and
           then use it.
        3. If cache_dir is not specified, a temporary directory will be used to fetch and store EFO cache. Note that
           this cannot be persistent, so the cache would have to be re-downloaded each time.
        """
        self.logger = logger

        # Initialize API clients.
        self._oxo = OxoClient()
        self._zooma = ZoomaClient()

        # Import EFO OWL datasets.
        if not cache_dir:
            cache_dir = tempfile.TemporaryDirectory().name
            self.logger.warning(
                f'EFO cache directory is not specified. Created temporary directory to store the cache: {cache_dir}. '
                f'We recommend that you specify the cache directory to speed up subsequent OnToma runs.'
            )
        if not os.path.isdir(cache_dir):
            os.mkdir(cache_dir)
            self.logger.info(f'Created EFO cache directory {cache_dir}.')
        if not os.listdir(cache_dir):
            self.logger.info(f'Downloading EFO cache to {cache_dir}.')
            owl.preprocess_owl(cache_dir)
        self.logger.info(f'Using EFO cache from {cache_dir}.')
        self.efo_terms = pd.read_csv(os.path.join(cache_dir, owl.TERMS_FILENAME), sep='\t')
        self.efo_xrefs = pd.read_csv(os.path.join(cache_dir, owl.XREFS_FILENAME), sep='\t')
        self.efo_synonyms = pd.read_csv(os.path.join(cache_dir, owl.SYNONYMS_FILENAME), sep='\t')
        self.logger.info(f'Loaded {len(self.efo_terms)} terms, {len(self.efo_xrefs)} xrefs, '
                         f'and {len(self.efo_synonyms)} synonyms from EFO cache.')

        # Import manually curated datasets.
        self.manual_xrefs = get_manual_xrefs(URLS['MANUAL_XREF'])
        self.manual_string = get_manual_string_mappings(URLS['MANUAL_STRING'])

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
        zooma_mappings = {
            ontology.normalise_ontology_identifier(mapping)
            for mapping in self._zooma.search(normalised_string)
        }
        return self.filter_identifiers_by_efo_current(zooma_mappings)

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
        self.logger.info(f'Processed: {query} → {result}')
        return result
