"""Disease curation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from ontoma.dataset.raw_entity_lut import RawEntityLUT
from ontoma.datasource.disease import OpenTargetsDisease
from ontoma.common.utils import (
    annotate_entity,
    get_alternative_translations,
    clean_disease_label
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DiseaseCuration:
    """Class to extract disease entities from a disease curation table."""

    @classmethod
    def as_label_lut(
        cls: type[DiseaseCuration], 
        disease_curation: DataFrame,
        curation_source: str,
        disease_index: DataFrame,
        remap_obsolete_mappings: bool = False
    ) -> RawEntityLUT:
        """Generate disease label lookup table from a disease curation table.

        Args:
            disease_curation (DataFrame): Input disease curation table.
            curation_source (str): Source of the curation table.
            disease_index (DataFrame): Open Targets disease index.
            remap_obsolete_mappings (bool): When True, obsolete mappings are remapped
                using the disease id lookup table created from the Open Targets disease index.
                Otherwise, only mappings in the disease index are retained. Default is False.
        
        Returns:
            RawEntityLUT: Disease label lookup table.
        """
        if remap_obsolete_mappings:
            disease_id_lut = OpenTargetsDisease.as_id_lut(disease_index)

            w = Window.partitionBy("entityLabel").orderBy(f.col("entityScore").desc())

            relevant_id_df = (
                # disease id lut contains entityId (id in disease index)
                # and entityLabel (ids/crossrefs/obsolete crossrefs)
                disease_id_lut.df
                # only keep highest ranked ids (if id itself is in disease index,
                # doesn't matter if it's a crossref of another id)
                .withColumn("entityRank", f.dense_rank().over(w))
                .filter(f.col("entityRank") == 1)
                # only keep rows where the entityLabel maps to exactly one id in the disease index
                .groupBy("entityLabel")
                .agg(
                    f.collect_set("entityId").alias("entityIds"),
                    f.size(f.collect_set("entityId")).alias("numEntityIds")
                )
                .filter(f.col("numEntityIds") == 1)
                .select(
                    # id in disease index
                    f.explode("entityIds").alias("entityId"),
                    # potential id found in curation table
                    f.regexp_replace(f.lower("entityLabel"), ":", "_").alias("entityIdToJoin")
                )
            )
        else:
            relevant_id_df = (
                disease_index
                .select(
                    f.col("id").alias("entityId"),
                    f.regexp_replace(f.lower("id"), ":", "_").alias("entityIdToJoin")
                )
            )

        return RawEntityLUT(
            _df=(
                disease_curation
                # extract entities from relevant fields and annotate entity with score and nlpPipelineTrack
                .select(
                    f.regexp_extract(
                        f.col("SEMANTIC_TAG"), r'^http.+/(\w+_\w+)$', 1
                    ).alias("entityId"),
                    annotate_entity(
                        # remove prefixes from curated disease labels
                        f.array(clean_disease_label(f.trim(f.col("PROPERTY_VALUE")))), "tbd", 1.0, curation_source
                    ).alias("curation")
                )
                # explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.col("curation")    
                    )
                )
                # select relevant fields and specify entity type
                .select(
                    f.lower("entityId").alias("entityIdToJoin"),
                    # translate non-latin alphabet characters, taking into account that
                    # labels that contain special characters should not always be translated
                    f.explode(
                        get_alternative_translations(
                            f.col("entity.entityLabel")
                        )
                    ).alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.col("entity.entitySource").alias("entitySource"),
                    f.lit("DS").alias("entityType"),
                    f.lit("label").alias("entityKind")
                )
                # only retain entityIds that are in the disease index
                # can be with/without remapping obsolete mappings (see above)
                .join(
                    relevant_id_df,
                    on="entityIdToJoin",
                    how="inner"
                )
                .drop("entityIdToJoin")
                # cleanup
                .filter(~f.col("entityLabel").rlike(r'^[12]\)$')) # specifically to remove "1)" and "2)"
                .filter((f.col("entityId").isNotNull()) & (f.length("entityId") > 0))
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
