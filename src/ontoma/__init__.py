"""Modules for OnToma."""

from __future__ import annotations

from ontoma.ontoma import OnToma
from ontoma.datasource.disease import OpenTargetsDisease
from ontoma.datasource.target import OpenTargetsTarget
from ontoma.datasource.drug import OpenTargetsDrug
from ontoma.datasource.disease_curation import DiseaseCuration

__all__ = [
    "OnToma",
    "OpenTargetsDisease",
    "OpenTargetsTarget",
    "OpenTargetsDrug",
    "DiseaseCuration"
]
