"""Open Targets disease index."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from ontoma.dataset.raw_entity_lut import RawEntityLUT
from ontoma.common.utils import (
    annotate_entity,
    get_alternative_translations,
    filter_disease_crossrefs,
    format_identifier
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class OpenTargetsDisease:
    """Class to extract disease entities from the Open Targets disease index."""

    @classmethod
    def as_label_lut(
        cls: type[OpenTargetsDisease], 
        disease_index: DataFrame
    ) -> RawEntityLUT:
        """Generate disease label lookup table from the Open Targets disease index.
        
        Args:
            disease_index (DataFrame): Open Targets disease index.

        Returns:
            RawEntityLUT: Disease label lookup table.
        """
        return RawEntityLUT(
            _df=(
                disease_index
                # extract entities from relevant fields and annotate entity with score and nlpPipelineTrack
                .select(
                    f.col("id").alias("entityId"),
                    annotate_entity(
                        f.array(f.col("name")), "tbd", 1.0, "name"
                    ).alias("name"),
                    annotate_entity(
                        f.col("synonyms.hasExactSynonym"), "tbd", 0.999, "exact_synonym"
                    ).alias("exactSynonyms"),
                    annotate_entity(
                        f.col("synonyms.hasNarrowSynonym"), "tbd", 0.998, "narrow_synonym"
                    ).alias("narrowSynonyms"),
                    annotate_entity(
                        f.col("synonyms.hasBroadSynonym"), "tbd", 0.997, "broad_synonym"
                    ).alias("broadSynonyms"),
                    annotate_entity(
                        f.col("synonyms.hasRelatedSynonym"), "tbd", 0.996, "related_synonym"
                    ).alias("relatedSynonyms")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(
                                f.col("name"),
                                f.col("exactSynonyms"),
                                f.col("narrowSynonyms"),
                                f.col("broadSynonyms"),
                                f.col("relatedSynonyms")
                            )
                        )
                    )
                )
                # select relevant fields and specify entity type
                .select(
                    f.col("entityId"),
                    # translate non-latin alphabet characters, taking into account that
                    # labels that contain special characters should not always be translated
                    f.explode(
                        get_alternative_translations(
                            f.trim(f.col("entity.entityLabel"))
                        )
                    ).alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.col("entity.entitySource").alias("entitySource"),
                    f.lit("DS").alias("entityType"),
                    f.lit("label").alias("entityKind")
                )
                # cleanup
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
    
    @classmethod
    def as_id_lut(
        cls: type[OpenTargetsDisease], 
        disease_index: DataFrame
    ) -> RawEntityLUT:
        """Generate disease id lookup table from the Open Targets disease index.

        Args:
            disease_index (DataFrame): Open Targets disease index.

        Returns:
            RawEntityLUT: Disease id lookup table.
        """
        return RawEntityLUT(
            _df=(
                disease_index
                # extract entities from relevant fields and annotate entity with score and nlpPipelineTrack
                .select(
                    f.col("id").alias("entityId"),
                    annotate_entity(
                        f.array(f.col("id")), "symbol", 1.0, "id"
                    ).alias("identifier"),
                    annotate_entity(
                        f.col("dbXRefs"), "symbol", 0.999, "crossref"
                    ).alias("crossRefs"),
                    annotate_entity(
                        f.col("obsoleteXRefs"), "symbol", 0.998, "obsolete_crossref"
                    ).alias("obsoleteCrossRefs")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(    
                                f.col("identifier"),
                                f.col("crossRefs"),
                                f.col("obsoleteCrossRefs")
                            )
                        )
                    )
                )
                # select relevant fields and specify entity type
                .select(
                    f.col("entityId"),
                    f.upper(f.trim(f.col("entity.entityLabel"))).alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.col("entity.entitySource").alias("entitySource"),
                    f.lit("DS").alias("entityType"),
                    f.lit("id").alias("entityKind")
                )
                # filter out disease crossrefs with irrelevant prefixes
                .transform(filter_disease_crossrefs)
                # format disease identifier to have consistent formatting
                .withColumn("entityLabel", format_identifier(f.col("entityLabel")))
                # cleanup
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
