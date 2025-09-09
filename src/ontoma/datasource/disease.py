"""Open Targets disease index."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from src.ontoma.dataset.raw_entity_lut import RawEntityLUT
from src.ontoma.common.utils import (
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
                        f.array(f.col("name")), 1.0, "term"
                    ).alias("nameTerm"),
                    annotate_entity(
                        f.array(f.col("name")), 1.0, "symbol"
                    ).alias("nameSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasExactSynonym"), 0.999, "term"
                    ).alias("exactSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasExactSynonym"), 0.999, "symbol"
                    ).alias("exactSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasNarrowSynonym"), 0.998, "term"
                    ).alias("narrowSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasNarrowSynonym"), 0.998, "symbol"
                    ).alias("narrowSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasBroadSynonym"), 0.997, "term"
                    ).alias("broadSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasBroadSynonym"), 0.997, "symbol"
                    ).alias("broadSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasRelatedSynonym"), 0.996, "term"
                    ).alias("relatedSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasRelatedSynonym"), 0.996, "symbol"
                    ).alias("relatedSynonymsSymbol")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(
                                f.col("nameTerm"),
                                f.col("nameSymbol"),
                                f.col("exactSynonymsTerm"),
                                f.col("exactSynonymsSymbol"),
                                f.col("narrowSynonymsTerm"),
                                f.col("narrowSynonymsSymbol"),
                                f.col("broadSynonymsTerm"),
                                f.col("broadSynonymsSymbol"),
                                f.col("relatedSynonymsTerm"),
                                f.col("relatedSynonymsSymbol")
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
                        f.array(f.col("id")), 1.0, "symbol"
                    ).alias("identifier"),
                    annotate_entity(
                        f.col("dbXRefs"), 0.999, "symbol"
                    ).alias("crossRefs"),
                    annotate_entity(
                        f.col("obsoleteXRefs"), 0.998, "symbol"
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
