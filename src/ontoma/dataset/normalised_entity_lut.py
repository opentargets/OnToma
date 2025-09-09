"""Normalised entity lookup table dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ontoma.common.schemas import parse_spark_schema
from ontoma.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class NormalisedEntityLUT(Dataset):
    """Normalised entity lookup table dataset.

    This dataset describes normalised entity lookup tables which result from 
    processing raw entity lookup tables using an NLP pipeline.
    """

    @classmethod
    def get_schema(cls: type[NormalisedEntityLUT]) -> StructType:
        """Provides the schema for the NormalisedEntityLUT dataset.

        Returns:
            StructType: Schema for the NormalisedEntityLUT dataset.
        """
        return parse_spark_schema("normalised_entity_lut.json")
