"""Functions that download mapping flat-files from github repo or other repositories."""

__all__ = [
    "get_ot_zooma_to_efo_mappings",
    "get_manual_xrefs"
    ]

import csv
import io
import logging
import requests

import pandas as pd

from ontoma.constants import URLS

logger = logging.getLogger(__name__)


def get_manual_xrefs(url):
    """Download the manual cross-reference list and convert to a Pandas dataframe."""
    logger.debug(f'OMIM to EFO mappings - requesting from URL {url}')
    return pd.read_csv(
        io.StringIO(
            requests.get(url).content.decode('utf-8')
        ),
        sep='\t'
    )


def get_ot_zooma_to_efo_mappings(url):
    """download zooma and returns a dict
    >>> d = get_ot_zooma_to_efo_mappings(URLS['ZOOMA_EFO_MAP'])
    >>> d['skeletal dysplasias']
    'http://purl.obolibrary.org/obo/HP_0002652'
    """
    mappings = {}
    logger.debug("ZOOMA to EFO mappings - requesting from URL %s", url)
    with requests.get(url, stream=True) as req:
        for i, row in enumerate(csv.DictReader(req.iter_lines(decode_unicode=True), delimiter='\t')):
            #(study, bioentity, property_type, property_value,
            # semantic_tag, annotator, annotation_date)
            # Note here should be 1:1 correspondence
            mappings[row['PROPERTY_VALUE'].lower()] = row['SEMANTIC_TAG'].strip()
        logger.info("ZOOMA to EFO mappings - Parsed %s rows", i)
    return mappings
