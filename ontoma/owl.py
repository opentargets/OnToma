import logging
import os
import urllib.request

import pandas as pd
import pronto
import requests

from ontoma import ontology

EFO_RELEASE_API = 'https://api.github.com/repos/EBISPOT/efo/releases/latest'

OWL_FILENAME = 'efo_otar_slim.owl'
TERMS_FILENAME = 'terms.tsv'
XREFS_FILENAME = 'xrefs.tsv'
SYNONYMS_FILENAME = 'synonyms.tsv'

logger = logging.getLogger(__name__)


def preprocess_owl(outdir):
    """Downloads and preprocesses the EFO OT slim CWL file. Outputs several tables in TSV format to the specified
    directory."""

    if not os.path.isdir(outdir):
        logging.info('Create the output directory.')
        os.mkdir(outdir)

    if not os.path.isfile(os.path.join(outdir, OWL_FILENAME)):
        logging.info('Download the EFO OT slim OWL file.')
        efo_assets = requests.get(EFO_RELEASE_API).json()['assets']
        otar_slim_assets = [a for a in efo_assets if a['name'] == OWL_FILENAME]
        assert len(otar_slim_assets) == 1, 'Exactly one efo_otar_slim.owl file is expected per EFO release.'
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
