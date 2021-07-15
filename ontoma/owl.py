import logging
import os
import urllib.request

import pandas as pd
import pronto
import requests
from retry import retry

from ontoma import ontology


EFO_RELEASE_API_TEMPLATE = 'https://api.github.com/repos/EBISPOT/efo/releases/{}'

OWL_FILENAME = 'efo_otar_slim.owl'
TERMS_FILENAME = 'terms.tsv'
XREFS_FILENAME = 'xrefs.tsv'
SYNONYMS_FILENAME = 'synonyms.tsv'
ALL_FILENAMES = {OWL_FILENAME, TERMS_FILENAME, XREFS_FILENAME, SYNONYMS_FILENAME}

logger = logging.getLogger(__name__)


@retry(logger=logger, tries=5, delay=3, backoff=1.5, jitter=(1, 3))
def fetch_efo_assets_from_github(efo_release):
    """Queries GitHub API to fetch the latest EFO release."""
    if efo_release == 'latest':
        url = EFO_RELEASE_API_TEMPLATE.format(efo_release)
    else:
        url = EFO_RELEASE_API_TEMPLATE.format(f'tags/{efo_release}')
    response = requests.get(url)
    response.raise_for_status()  # In case of HTTP errors, this will be caught by the @retry decorator.
    return response.json()['assets']


def preprocess_owl(outdir, efo_release):
    """Downloads and preprocesses the EFO OT slim CWL file. Outputs several tables in TSV format to the specified
    directory."""

    if not os.path.isdir(outdir):
        logging.info('Create the output directory.')
        os.mkdir(outdir)

    if not os.path.isfile(os.path.join(outdir, OWL_FILENAME)):
        logging.info('Download the EFO OT slim OWL file.')
        efo_assets = fetch_efo_assets_from_github(efo_release)
        otar_slim_assets = [a for a in efo_assets if a['name'] == OWL_FILENAME]
        if len(otar_slim_assets) == 0:
            raise AssertionError(f'EFO release {efo_release!r} on GitHub does not contain the file {OWL_FILENAME!r}.')
        if len(otar_slim_assets) > 1:
            raise AssertionError(f'EFO release {efo_release!r} contains multiple files named {OWL_FILENAME!r}.')
        otar_slim_url = otar_slim_assets[0]['browser_download_url']
        urllib.request.urlretrieve(otar_slim_url, os.path.join(outdir, OWL_FILENAME))

    logging.info('Process the OWL file.')
    terms_dataset, xrefs_dataset, synonyms_dataset = [], [], []
    for term in pronto.Ontology(os.path.join(outdir, OWL_FILENAME)).terms():
        normalised_term_id = ontology.normalise_ontology_identifier(term.id)
        terms_dataset.append([
            normalised_term_id,
            term.name.lower() if term.name else '',
            term.obsolete,
        ])
        for xref in term.xrefs:
            normalised_xref_id = ontology.normalise_ontology_identifier(xref.id)
            if normalised_xref_id:
                xrefs_dataset.append([
                    normalised_xref_id,
                    normalised_term_id,
                ])
        for synonym in term.synonyms:
            if synonym.description and synonym.scope == 'EXACT':
                synonyms_dataset.append([
                    synonym.description.lower(),
                    normalised_term_id,
                ])

    logging.info('Output the datasets.')
    pd.DataFrame(
        terms_dataset, columns=('normalised_id', 'normalised_label', 'is_obsolete')
    ).to_csv(os.path.join(outdir, TERMS_FILENAME), sep='\t', index=False)
    pd.DataFrame(
        xrefs_dataset, columns=('normalised_xref_id', 'normalised_id',)
    ).to_csv(os.path.join(outdir, XREFS_FILENAME), sep='\t', index=False)
    pd.DataFrame(
        synonyms_dataset, columns=('normalised_synonym', 'normalised_id')
    ).to_csv(os.path.join(outdir, SYNONYMS_FILENAME), sep='\t', index=False)
