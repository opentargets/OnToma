"""OnToma class for ontology mapping."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from ontoma.common.utils import (
    get_alternative_translations,
    clean_disease_label,
    format_identifier
)
from ontoma.dataset.raw_entity_lut import RawEntityLUT
from ontoma.dataset.normalised_entity_lut import NormalisedEntityLUT
from ontoma.dataset.ready_entity_lut import ReadyEntityLUT
from ontoma.nlp_pipeline import NLPPipeline

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


@dataclass
class OnToma:
    """Class to initialise an entity lookup table for mapping entities."""

    spark: SparkSession
    entity_lut_list: list[RawEntityLUT]
    cache_dir: str | None = None
    _entity_lut: ReadyEntityLUT | None = field(init=False, default=None)

    def __post_init__(self: OnToma) -> None:
        """Post init.

        Initialises an entity lookup table for mapping entities using the list of entity lookup tables provided.
        If a cache directory is specified, it will load cached data or save to cache.

        Raises:
            ValueError: When entity_lut_list is empty or when required Spark NLP configuration is missing.
            TypeError: When entity_lut_list is not a list or when elements of entity_lut_list are not RawEntityLUT.
        """
        # check for required spark config
        if not self._check_spark_config(
            self.spark,
            "spark.jars.packages",
            "com.johnsnowlabs.nlp:spark-nlp_2.12:5.0.0"
        ):
            raise ValueError("Spark session is missing configuration required for Spark NLP.")

        # validate the input
        if not isinstance(self.entity_lut_list, list):
            raise TypeError("entity_lut_list must be a list.")
        
        if not self.entity_lut_list:
            raise ValueError("entity_lut_list must contain at least one element.")
        
        if not all(isinstance(entity_lut, RawEntityLUT) for entity_lut in self.entity_lut_list):
            raise TypeError("Each entity_lut must be a RawEntityLUT.")

        # if cache_dir is provided and it exists, load the entity lookup table
        if self.cache_dir and os.path.exists(self.cache_dir):
            self._entity_lut = ReadyEntityLUT(
                _df=self.spark.read.parquet(self.cache_dir),
                _schema=ReadyEntityLUT.get_schema()
            )
            logger.info(f"Loaded entity lookup table from {self.cache_dir}.")

        # if cache_dir is provided but doesn't exist yet, generate and save the entity lookup table
        elif self.cache_dir:
            logger.info(f"{self.cache_dir} does not exist. Generating entity lookup table.")
            self._entity_lut = self._generate_entity_lut(self.entity_lut_list)

            self._entity_lut.df.write.parquet(self.cache_dir)
            logger.info(f"Saved entity lookup table to {self.cache_dir}.")
            
        # cache_dir is not provided so just generate the entity lookup table
        else:
            logger.warning(f"Cache directory is not specified. Specify a cache directory to speed up subsequent OnToma usage.")
            logger.info(f"Generating entity lookup table.")
            self._entity_lut = self._generate_entity_lut(self.entity_lut_list)

    @property
    def df(self: OnToma) -> DataFrame:
        """Entity lookup table initialised in the post init.

        Returns:
            DataFrame: Entity lookup table initialised in the post init.
        """
        return self._entity_lut.df
    
    @staticmethod
    def _check_spark_config(spark: SparkSession, config_key: str, expected_value: str) -> bool:
        """Checks if the spark session has the required configuration set with the expected value.

        Args:
            spark (SparkSession): Spark session.
            config_key (str): Required Spark configuration key.
            expected_value (str): Expected Spark configuration value.

        Returns:
            bool: True if the required configuration is set with the expected value, False otherwise.
        """
        try:
            actual_value = spark.conf.get(config_key)
        except Exception:
            return False
        return actual_value == expected_value

    def _generate_entity_lut(self: OnToma, lut_list: list[RawEntityLUT]) -> ReadyEntityLUT:
        """Wrapper containing logic for generating an entity lookup table ready to be used for entity mapping given a list of raw entity lookup tables.

        Args:
            lut_list (list[RawEntityLUT]): List of raw entity lookup tables.

        Returns:
            ReadyEntityLUT: Entity lookup table ready to be used for entity mapping.
        """
        # concatenate entity lookup tables for downstream processing
        raw_entity_lut = self._concatenate_entity_luts(lut_list)

        # normalise the entity lookup table using an NLP pipeline
        normalised_entity_lut = self._normalise_entity_lut(raw_entity_lut)

        # post-processing to get relevant entity ids for each entity label
        return self._get_relevant_entity_ids(normalised_entity_lut)

    @staticmethod
    def _concatenate_entity_luts(lut_list: list[RawEntityLUT]) -> RawEntityLUT:
        """Concatenate raw entity lookup tables.

        Args:
            lut_list (list[RawEntityLUT]): List of raw entity lookup tables to be concatenated.

        Returns:
            RawEntityLUT: Concatenated raw entity lookup table.
        """
        if len(lut_list) == 1:
            return lut_list[0]
        
        return reduce(lambda lut1, lut2: RawEntityLUT(lut1.df.unionByName(lut2.df)), lut_list)

    def _normalise_entity_lut(self: OnToma, raw_entity_lut: RawEntityLUT) -> NormalisedEntityLUT:
        """Wrapper for applying the _normalise_entities function to entity lookup tables.

        Args:
            raw_entity_lut (RawEntityLUT): Raw entity lookup table containing entity labels to be normalised.

        Returns:
            NormalisedEntityLUT: Normalised entity lookup table containing normalised entity labels.
        """
        return NormalisedEntityLUT(
            _df=(
                self._normalise_entities(raw_entity_lut.df)
                .filter(f.col("entityLabelNormalised").isNotNull() & (f.length("entityLabelNormalised") > 0))
            ),
            _schema=NormalisedEntityLUT.get_schema()
        )
    
    @staticmethod
    def _normalise_entities(df: DataFrame) -> DataFrame:
        """Normalise entities using an NLP pipeline.

        The output column selected is determined by the NLP pipeline type specified.

        Args:
            df (DataFrame): DataFrame containing entity labels to be normalised.

        Returns:
            DataFrame: DataFrame with additional column containing normalised entity labels.
        """
        normalised_entities = NLPPipeline.apply_pipeline(df, "entityLabel")

        return (
            normalised_entities
            .withColumn(
                "entityLabelNormalised",
                f.when(
                    f.col("nlpPipelineTrack") == "term",
                    f.array_join(
                        f.array_sort(
                            f.filter(
                                f.array_distinct(f.col("finished_term")),
                                lambda c: c.isNotNull() & (c != "")
                            )
                        ),
                        ""
                    )
                ).when(
                    f.col("nlpPipelineTrack") == "symbol",
                    f.array_join(
                        f.filter(
                            f.col("finished_symbol"), 
                            lambda c: c.isNotNull() & (c != "")
                        ),
                        ""
                    )
                )
            )
            .drop("finished_term", "finished_symbol") #, "nlpPipelineTrack", "entityLabel")
        )
    
    def _map_entities_substring(
        self: OnToma,
        normalised_query_entities: DataFrame,
        type_col_name: str
    ) -> DataFrame:
        """Map entities using substring matching instead of exact matching.
        
        This method finds all lookup table entries where the normalised lookup label
        is a substring of the normalised query label, or vice versa.

        !!! note
            This method is computationally more expensive and may lead to false positives.
        
        Args:
            normalised_query_entities (DataFrame): Normalised query entities from NLP pipeline.
            type_col_name (str): Name of the type column.
            
        Returns:
            DataFrame: Mapped entities with substring matches.
        """
        # Get the lookup table with proper aliases
        lookup_df = (
            self.df
            .select(
                f.col("entityLabelNormalised").alias("lookup_label_normalised"),
                f.col("entityType").alias(f"lookup_{type_col_name}"),
                f.col("entityKind").alias("lookup_entityKind"),
                f.col("entityIds")
            )
        )
        
        # Perform cross join and then filter for substring matches
        # This is more efficient than a full cross join because we filter on entityType and entityKind first
        return (
            normalised_query_entities
            .crossJoin(lookup_df)
            # Filter for matching type and kind first (most selective)
            .filter(
                (f.col(type_col_name) == f.col(f"lookup_{type_col_name}")) &
                (f.col("entityKind") == f.col("lookup_entityKind"))
            )
            # Filter for substring matches: either direction
            .filter(
                f.col("entityLabelNormalised").contains(f.col("lookup_label_normalised")) |
                f.col("lookup_label_normalised").contains(f.col("entityLabelNormalised"))
            )
            # Select the original structure expected by the downstream pipeline
            .select(
                *[col for col in normalised_query_entities.columns],  # Keep all original query columns
                f.col("entityIds")
            )
        )
    
    @staticmethod
    def _get_relevant_entity_ids(normalised_entity_lut: NormalisedEntityLUT) -> ReadyEntityLUT:
        """Get relevant entity ids for each entity label.

        Args:
            normalised_entity_lut (NormalisedEntityLUT): Normalised entity lookup table containing all entity ids for each entity label.

        Returns:
            ReadyEntityLUT: Entity lookup table containing only the relevant entity ids for each entity label, ready to be used for entity mapping.
        """
        w = Window.partitionBy("entityKind", "entityType", "entityLabelNormalised").orderBy(f.col("entityScore").desc())

        return ReadyEntityLUT(
            _df=(
                normalised_entity_lut.df
                .withColumn("entityRank", f.dense_rank().over(w))
                .filter(f.col("entityRank") == 1)
                .groupBy("entityKind", "entityType", "entityLabelNormalised")
                .agg(f.collect_set(f.col("entityId")).alias("entityIds"))
            ),
            _schema=ReadyEntityLUT.get_schema()
        )
    
    @staticmethod
    def _check_mapping_compatibility(
        lut: DataFrame, 
        df: DataFrame, 
        lut_col_name: str,
        df_col_name: str
    ) -> bool:
        """Check if the entity lookup table can be used to map the entities in the provided dataframe.

        Args:
            lut (DataFrame): The entity lookup table.
            df (DataFrame): The provided dataframe.
            lut_col_name (str): Name of the column containing the entity property in the entity lookup table.
            df_col_name (str): Name of the column containing the entity property in the provided dataframe.

        Returns:
            bool: True if all the entity properties are in the entity lookup table, False otherwise.
        """
        lut_properties = lut.select(lut_col_name).distinct().collect()

        df_properties = df.select(df_col_name).distinct().collect()

        return all(val in lut_properties for val in df_properties)
    
    @staticmethod
    def _extract_query_entity_labels(
        df: DataFrame,
        label_col_name: str,
        type_col_name: str,
    ) -> DataFrame:
        """Extract query entity labels from the provided dataframe.

        Entity labels are set up for normalisation via both the term and symbol tracks of the NLP pipeline.

        Args:
            df (DataFrame): DataFrame containing entity labels to be extracted.
            label_col_name (str): Name of the column containing the entity labels.
            type_col_name (str): Name of the column containing the type of the entity label.
        
        Returns:
            DataFrame: DataFrame with additional columns containing entity string and NLP pipeline track.
        """
        return (
            df
            # translate non-latin alphabet characters, taking into account that
            # labels that contain special characters should not always be translated
            .withColumn(
                "entityLabel",
                f.explode(get_alternative_translations(f.trim(f.col(label_col_name))))
            )
            # all query entities will be normalised using both the term and symbol tracks of the NLP pipeline
            .withColumn(
                "nlpPipelineTrack",
                f.explode(f.array(f.lit("term"), f.lit("symbol")))
            )
            # disease labels require an additional cleaning step
            .withColumn(
                "entityLabel",
                f.when(f.col(type_col_name) == "DS", clean_disease_label(f.col("entityLabel")))
                .otherwise(f.col("entityLabel"))
            )
        )
    
    @staticmethod
    def _extract_query_entity_ids(
        df: DataFrame,
        id_col_name: str
    ) -> DataFrame:
        """Extract query entity ids from the provided dataframe.

        Entity ids are set up for normalisation via the symbol track of the NLP pipeline.

        Args:
            df (DataFrame): DataFrame containing entity ids to be extracted.
            id_col_name (str): Name of the column containing the entity ids.

        Returns:
            DataFrame: DataFrame with additional columns containing entity string and NLP pipeline track.
        """
        return (
            df
            .withColumns(
                {
                    # format ids to be consistent
                    "entityLabel": format_identifier(f.upper(f.trim(f.col(id_col_name)))),
                    # all query ids will be normalised using the symbol track of the NLP pipeline
                    "nlpPipelineTrack": f.lit("symbol")
                }
            )
        )

    def map_entities(
        self: OnToma, 
        df: DataFrame, 
        result_col_name: str,
        entity_col_name: str, 
        entity_kind: str,
        type_col_name: str | None = None, 
        type_col: Column | None = None,
        map_substring: bool = False
     ) -> DataFrame:
        """Map entities using the entity lookup table.

        Logic:
        1. Extract entities from input dataframe.
        2. Normalise entities using both tracks of the NLP pipeline.
        3. Join with entity lookup table (exact match or substring match).
        4. Aggregate results from both tracks.

        Args:
            df (DataFrame): DataFrame containing entity labels to be mapped.
            result_col_name (str): Name of the column for the result.
            entity_col_name (str): Name of the column containing the entity labels.
            entity_kind (str): Kind (label or id) of the entity label.
            type_col_name (str | None): Name of the column containing the type of the entity label.
            type_col (Column | None): Column containing the type of the entity label.
            map_substring (bool): If True, perform substring matching instead of exact matching. Defaults to False.

        Returns:
            DataFrame: DataFrame with additional column containing a list of relevant entity ids for each entity label.

        Raises:
            ValueError: When both or none of 'type_col_name' or 'type_col' are provided,
                or when the input dataframe contains unmappable entity types.
        """
        # validate input for the type column
        if (
            (type_col_name is None and type_col is None) 
            or (type_col_name is not None and type_col is not None)
        ):
            raise ValueError("Exactly one of 'type_col_name' or 'type_col' must be provided.")
        
        # create snapshot of columns from input dataframe
        original_columns = df.columns
        
        # if type information is provided as a Column, add it to the input dataframe
        if type_col is not None:
            type_col_name = "entityType"
            df = df.withColumn(type_col_name, type_col)

        # check if all the entity types to be mapped are in the entity lookup table
        if not self._check_mapping_compatibility(self.df, df, "entityType", type_col_name):
            raise ValueError("Unable to map the provided entity type(s).")
        
        # add kind information to the input dataframe
        df = df.withColumn("entityKind", f.lit(entity_kind))

        # check if all the entity kinds to be mapped are in the entity lookup table
        if not self._check_mapping_compatibility(self.df, df, "entityKind", "entityKind"):
            raise ValueError("Unable to map the provided entity kind(s).")
    
        # extract entities from input dataframe
        if entity_kind == "label":
            extracted_entities = self._extract_query_entity_labels(df, entity_col_name, type_col_name)
        if entity_kind == "id":
            extracted_entities = self._extract_query_entity_ids(df, entity_col_name)

        # normalise entities and join with entity lookup table
        if map_substring:
            mapped_entities = self._map_entities_substring(
                self._normalise_entities(extracted_entities), type_col_name
            )
        else:
            mapped_entities = (
                self._normalise_entities(extracted_entities)
                .join(
                    (
                        self.df
                        .select(
                            f.col("entityLabelNormalised"),
                            f.col("entityType").alias(type_col_name),
                            f.col("entityKind"),
                            f.col("entityIds")
                        )
                    ),
                    on=["entityLabelNormalised", type_col_name, "entityKind"],
                    how="left"
                )
            )

        # aggregate results from both tracks
        return (
            mapped_entities
            .groupBy(original_columns)
            .agg(f.array_distinct(f.flatten(f.collect_set(f.col("entityIds")))).alias(result_col_name))
            # replace empty list with null
            .withColumn(
                result_col_name,
                f.when(f.size(result_col_name) == 0, None)
                .otherwise(f.col(result_col_name))
            )
        )
