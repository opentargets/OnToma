"""Contains utilities for handling ontology terms and normalising the identifier representation.

The idea behind this module is that any incoming ontology representation (from query, EFO OWL file etc.) is first
converted into internal, consistent representation. Then direct string-to-string comparisons can be made without having
to adjust for different ontology representation methods. For example, the following identifiers: ORDO_140162,
ORPHA:140162, Orphanet:140162 are all consistently converted into ORDO:140162.

The normalised identifiers can then be converted into other formats for exporting the data or making external queries,
for example:
* The Open Targets schema format: Orphanet_140162;
* Full URI for querying OLS: http://www.orpha.net/ORDO/Orphanet_140162."""

import re
from typing import Optional

# Some ontologies (most notably Orphanet) have multiple labels for specifying the same namespace.
NORMALISATION_MAPPING = {
    'ORPHA': 'ORDO',
    'ORPHANET': 'ORDO',
}


def normalise_ontology_identifier(identifier: str) -> Optional[str]:
    """Normalise ontology identifier representation in order to make direct string-to-string comparison possible."""
    if 'identifiers.org' in identifier:
        # If this is an identifiers.org link, extract ontology name and identifier from the last two elements.
        # http://identifiers.org/omim/137215" → [omim, 137215].
        ontology_name, ontology_id = identifier.split('/')[-2:]
    else:
        # For any other type of link (EFO, OBO, Orphanet), extract ontology name and identifier from the last element.
        # http://www.orpha.net/ORDO/Orphanet_140162 → Orphanet_140162.
        identifier = identifier.split('/')[-1]
        # Split into ontology name and identifier parts, separated by either colon or underscore.
        # Orphanet_140162 → [Orphanet, 140162].
        parts = re.split('[:_]', identifier)
        if len(parts) != 2:  # Not a valid/supportable ontology identifier.
            return None
        ontology_name, ontology_id = parts

    # Convert ontology name to uppercase and substitute, if necessary.
    # [Orphanet, 140162] → [ORDO, 140162].
    ontology_name = ontology_name.upper()
    ontology_name = NORMALISATION_MAPPING.get(ontology_name, ontology_name)

    # Glue ontology name and identifier back together into a CURIE. This can be queried in most contexts and tools.
    # [ORDO, 140162] → ORDO:140162.
    return f'{ontology_name}:{ontology_id}'


OT_SCHEMA_MAPPING = {
    'ORDO': 'Orphanet'
}
OT_SCHEMA_MAPPING.update({
    ontology: ontology for ontology in ('NCIT', 'GO', 'HP', 'EFO', 'MONDO', 'DOID', 'MP')
})
# While OTAR is listed in the schema, it is not included, because it is only used for classes and never for individual
# terms.


def convert_to_ot_schema(normalised_identifier: str) -> Optional[str]:
    """Convert normalised ontology representation into the one accepted by the Open Targets JSON schema. For example,
    ORDO:140162 → Orphanet_140162. If the target ontology is not supported by the schema, return None."""
    ontology_name, ontology_id = normalised_identifier.split(':')
    ontology_name = OT_SCHEMA_MAPPING.get(ontology_name)
    if ontology_name:
        return f'{ontology_name}_{ontology_id}'
    else:
        return None


URI_MAPPING = {
    'NCIT':  'http://purl.obolibrary.org/obo/NCIT_{}',
    'ORDO':  'http://www.orpha.net/ORDO/Orphanet_{}',
    'GO':    'http://purl.obolibrary.org/obo/GO_{}',
    'HP':    'http://purl.obolibrary.org/obo/HP_{}',
    'EFO':   'http://www.ebi.ac.uk/efo/EFO_{}',
    'MONDO': 'http://purl.obolibrary.org/obo/MONDO_{}',
    'DOID':  'http://purl.obolibrary.org/obo/DOID_{}',
    'MP':    'http://purl.obolibrary.org/obo/MP_{}',
}


def convert_to_uri(normalised_identifier: str) -> Optional[str]:
    """Convert normalised ontology representation into a full URI. If the target ontology is not supported by the
    schema, return None."""
    ontology_name, ontology_id = normalised_identifier.split(':')
    uri_template = URI_MAPPING.get(ontology_name)
    if uri_template:
        return uri_template.format(ontology_id)
    else:
        return None
