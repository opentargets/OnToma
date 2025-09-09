"""Dataset class for OnToma."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import types as t
from typing_extensions import Self

from src.ontoma.common.schemas import SchemaValidationError, compare_struct_schemas

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class Dataset(ABC):
    """Open Targets OnToma Dataset Interface.

    The `Dataset` interface is a wrapper around a Spark DataFrame with a predefined schema.
    Class allows for overwriting the schema with `_schema` parameter.
    If the `_schema` is not provided, the schema is inferred from the Dataset.get_schema specific
    method which must be implemented by the child classes.
    """

    _df: DataFrame
    _schema: StructType | None = None

    def __post_init__(self: Dataset) -> None:
        """Post init.

        Raises:
            TypeError: If the type of the _df or _schema is not valid
        """
        match self._df:
            case DataFrame():
                pass
            case _:
                raise TypeError(f"Invalid type for _df: {type(self._df)}")

        match self._schema:
            case None | t.StructType():
                self.validate_schema()
            case _:
                raise TypeError(f"Invalid type for _schema: {type(self._schema)}")

    @property
    def df(self: Dataset) -> DataFrame:
        """Dataframe included in the Dataset.

        Returns:
            DataFrame: Dataframe included in the Dataset
        """
        return self._df
    
    @df.setter
    def df(self: Dataset, new_df: DataFrame) -> None:
        """Dataframe setter.

        Args:
            new_df (DataFrame): New dataframe to be included in the Dataset
        """
        self._df: DataFrame = new_df
        self.validate_schema()

    @property
    def schema(self: Dataset) -> StructType:
        """Dataframe expected schema.

        Returns:
            StructType: Dataframe expected schema
        """
        return self._schema or self.get_schema()

    @classmethod
    @abstractmethod
    def get_schema(cls: type[Self]) -> StructType:
        """Abstract method to get the schema. Must be implemented by child classes.

        Returns:
            StructType: Schema for the Dataset

        Raises:
                NotImplementedError: Must be implemented in the child classes
        """
        raise NotImplementedError("Must be implemented in the child classes")
    
    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            SchemaValidationError: If the DataFrame schema does not match the expected schema
        """
        expected_schema = self.schema
        observed_schema = self._df.schema

        # Unexpected fields in dataset
        if discrepancies := compare_struct_schemas(observed_schema, expected_schema):
            raise SchemaValidationError(
                f"Schema validation failed for {type(self).__name__}", discrepancies
            )
    