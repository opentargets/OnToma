"""NER-based entity extraction for OnToma preprocessing.

This module provides Named Entity Recognition (NER) capabilities for extracting
clean entity names from raw text labels before mapping them to ontology IDs.
"""

from __future__ import annotations

from ontoma.ner.drug import extract_drug_entities
from ontoma.ner._pipelines import create_ner_pipeline

__all__ = ["extract_drug_entities", "create_ner_pipeline"]
