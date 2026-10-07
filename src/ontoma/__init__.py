"""Modules for OnToma."""

from __future__ import annotations

from ontoma.ontoma import OnToma
from ontoma.datasource.disease import OpenTargetsDisease
from ontoma.datasource.target import OpenTargetsTarget
from ontoma.datasource.drug import OpenTargetsDrug
from ontoma.datasource.disease_curation import DiseaseCuration
from ontoma.spark_nlp import spark_nlp_coordinate

__all__ = [
    "OnToma",
    "OpenTargetsDisease",
    "OpenTargetsTarget",
    "OpenTargetsDrug",
    "DiseaseCuration",
    "spark_nlp_coordinate",
]
