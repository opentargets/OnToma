"""Raw entity lookup table dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ontoma.common.schemas import parse_spark_schema
from ontoma.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class RawEntityLUT(Dataset):
    """Raw entity lookup table dataset.

    This dataset describes raw entity lookup tables which result from 
    extracting entities from a datasource.
    """

    @classmethod
    def get_schema(cls: type[RawEntityLUT]) -> StructType:
        """Provides the schema for the RawEntityLUT dataset.

        Returns:
            StructType: Schema for the RawEntityLUT dataset.
        """
        return parse_spark_schema("raw_entity_lut.json")
