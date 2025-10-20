"""Ready entity lookup table dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ontoma.common.schemas import parse_spark_schema
from ontoma.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class ReadyEntityLUT(Dataset):
    """Ready entity lookup table dataset.

    This dataset describes ready entity lookup tables which are
    ready to be used for entity mapping.
    """

    @classmethod
    def get_schema(cls: type[ReadyEntityLUT]) -> StructType:
        """Provides the schema for the ReadyEntityLUT dataset.

        Returns:
            StructType: Schema for the ReadyEntityLUT dataset.
        """
        return parse_spark_schema("ready_entity_lut.json")
