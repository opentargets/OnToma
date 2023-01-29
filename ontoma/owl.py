"""Class to handle EFO ontology related functions."""

import logging
import os
import urllib.request

import pandas as pd
import pronto
import requests
from retry import retry
from typing import Dict

from ontoma import ontology

logger = logging.getLogger(__name__)


class efo_handler:
    """A collection of functions to manage EFO data retrieve and version control and caching."""

    EFO_RELEASE_API_TEMPLATE = "https://api.github.com/repos/EBISPOT/efo/releases/{}"

    def __init__(self, efo_release: str, cache_dir: str) -> None:
        """By providing and EFO version and cache dir, the class tries to load cached data or create new cache.

        Args:
            efo_release (str): EFO version. Must be a valid release tag.
            cache_dir (str): Folder into which the cache dataset will be saved.
        """
        # Get EFO release related meta data:
        efo_release_info = self.__get_efo_version(efo_release)
        self.efo_version = efo_release_info["tag_name"]
        self.assets = efo_release_info["assets"]
        self.cache_dir = cache_dir

        # Fix filenames:
        self.OWL_FILENAME = "efo_otar_slim.owl"
        self.TERMS_FILENAME = f"terms.{self.efo_version}.tsv"
        self.XREFS_FILENAME = f"xrefs.{self.efo_version}.tsv"
        self.SYNONYMS_FILENAME = f"synonyms.{self.efo_version}.tsv"

        # datasets:
        self.ALL_DATASETS = {
            self.TERMS_FILENAME: "terms_df",
            self.XREFS_FILENAME: "xrefs_df",
            self.SYNONYMS_FILENAME: "synonyms_df",
        }

        # If cache dir doesn't exist, create it:
        if not os.path.isdir(cache_dir):
            os.mkdir(cache_dir)
            logger.info(f"Created EFO cache directory {cache_dir}.")

        # Try to load all the files, if any of the file is missing, rebuilding cache:
        for filename, dataset_name in self.ALL_DATASETS.items():
            try:
                setattr(
                    self,
                    dataset_name,
                    pd.read_csv(os.path.join(cache_dir, filename), sep="\t"),
                )
            except FileNotFoundError:
                logger.info(f"{filename} was not found. Re-building cache.")
                self.__generate_cache()
                break

    def __generate_cache(self) -> None:
        otar_slim_assets = [a for a in self.assets if a["name"] == self.OWL_FILENAME]
        if len(otar_slim_assets) == 0:
            raise AssertionError(
                f"EFO release {self.efo_version!r} on GitHub does not contain the file {self.OWL_FILENAME!r}."
            )
        if len(otar_slim_assets) > 1:
            raise AssertionError(
                f"EFO release {self.efo_version!r} contains multiple files named {self.OWL_FILENAME!r}."
            )
        otar_slim_url = otar_slim_assets[0]["browser_download_url"]

        # Retrieve OWL file to cache dir:
        logging.info(f"Fetching the OWL file ({self.OWL_FILENAME}).")
        urllib.request.urlretrieve(
            otar_slim_url, os.path.join(self.cache_dir, self.OWL_FILENAME)
        )

        # Processing owl file:
        logging.info("Processing the OWL file.")
        terms_dataset, xrefs_dataset, synonyms_dataset = [], [], []

        # Reading ontology:
        for term in pronto.Ontology(
            os.path.join(self.cache_dir, self.OWL_FILENAME)
        ).terms():
            normalised_term_id = ontology.normalise_ontology_identifier(term.id)
            terms_dataset.append(
                [
                    normalised_term_id,
                    term.name.lower() if term.name else "",
                    term.obsolete,
                ]
            )
            # Extracting cross references:
            for xref in term.xrefs:
                normalised_xref_id = ontology.normalise_ontology_identifier(xref.id)
                if normalised_xref_id:
                    xrefs_dataset.append(
                        [
                            normalised_xref_id,
                            normalised_term_id,
                        ]
                    )
            # Extracting synonyms:
            for synonym in term.synonyms:
                if synonym.description and synonym.scope == "EXACT":
                    synonyms_dataset.append(
                        [
                            synonym.description.lower(),
                            normalised_term_id,
                        ]
                    )

        logging.info("Output the datasets.")

        # Strore datasets list:
        self.terms_df = pd.DataFrame(
            terms_dataset, columns=("normalised_id", "normalised_label", "is_obsolete")
        )
        self.xrefs_df = pd.DataFrame(
            xrefs_dataset,
            columns=(
                "normalised_xref_id",
                "normalised_id",
            ),
        )
        self.synonyms_df = pd.DataFrame(
            synonyms_dataset, columns=("normalised_synonym", "normalised_id")
        )

        # Save datasets
        self.terms_df.to_csv(
            os.path.join(self.cache_dir, self.TERMS_FILENAME), sep="\t", index=False
        )
        self.xrefs_df.to_csv(
            os.path.join(self.cache_dir, self.XREFS_FILENAME), sep="\t", index=False
        )
        self.synonyms_df.to_csv(
            os.path.join(self.cache_dir, self.SYNONYMS_FILENAME), sep="\t", index=False
        )

    def get_terms(self) -> pd.DataFrame:
        """This function returns the EFO term list as a pandas DataFrame."""
        return self.terms_df

    def get_xrefs(self) -> pd.DataFrame:
        """This function returns the EFO term cross references as a pandas DataFrame."""
        return self.xrefs_df

    def get_synonyms(self) -> pd.DataFrame:
        """This function returns the EFO term synonyms as pandas DataFrame."""
        return self.synonyms_df

    @retry(logger=logger, tries=5, delay=3, backoff=1.5, jitter=(1, 3))
    def __get_efo_version(self, efo_release: str) -> Dict[str, str]:
        """Queries GitHub API to fetch the latest EFO release."""
        if efo_release == "latest":
            url = self.EFO_RELEASE_API_TEMPLATE.format(efo_release)
        else:
            url = self.EFO_RELEASE_API_TEMPLATE.format(f"tags/{efo_release}")
        response = requests.get(url)
        response.raise_for_status()  # In case of HTTP errors, this will be caught by the @retry decorator.
        return response.json()


efo = efo_handler(release, cache_dir)
