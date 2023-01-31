"""Functions that download mapping flat files from github repo or other repositories."""

__all__ = [
    'get_manual_xrefs',
    'get_manual_string_mappings',
]

import logging

import pandas as pd

from ontoma.ontology import normalise_ontology_identifier

logger = logging.getLogger(__name__)


def get_manual_xrefs(url: str) -> pd.DataFrame:
    """Download the manual cross-reference list and convert to a Pandas dataframe."""
    logger.debug(f'Requesting manual ontology-to-EFO mappings from URL {url}')
    df = pd.read_csv(url, sep='\t')
    df.normalised_xref_id = df.normalised_xref_id.apply(normalise_ontology_identifier)
    df.normalised_id = df.normalised_id.apply(normalise_ontology_identifier)
    return df


def get_manual_string_mappings(url: str) -> pd.DataFrame:
    """Download the manual string-to-ontology mapping list and convert to a Pandas dataframe."""
    logger.debug(f'Requesting manual string-to-EFO mappings from URL {url}')
    df = pd.read_csv(url, sep='\t')
    df['normalised_label'] = df.PROPERTY_VALUE.apply(str.lower)
    df['normalised_id'] = df.SEMANTIC_TAG.apply(normalise_ontology_identifier)
    return df[['normalised_label', 'normalised_id']]
