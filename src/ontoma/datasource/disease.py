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
                        f.array(f.col("name")), 1.0, "term", "name"
                    ).alias("nameTerm"),
                    annotate_entity(
                        f.array(f.col("name")), 1.0, "symbol", "name"
                    ).alias("nameSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasExactSynonym"), 0.999, "term", "exact_synonym"
                    ).alias("exactSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasExactSynonym"), 0.999, "symbol", "exact_synonym"
                    ).alias("exactSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasNarrowSynonym"), 0.998, "term", "narrow_synonym"
                    ).alias("narrowSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasNarrowSynonym"), 0.998, "symbol", "narrow_synonym"
                    ).alias("narrowSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasBroadSynonym"), 0.997, "term", "broad_synonym"
                    ).alias("broadSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasBroadSynonym"), 0.997, "symbol", "broad_synonym"
                    ).alias("broadSynonymsSymbol"),
                    annotate_entity(
                        f.col("synonyms.hasRelatedSynonym"), 0.996, "term", "related_synonym"
                    ).alias("relatedSynonymsTerm"),
                    annotate_entity(
                        f.col("synonyms.hasRelatedSynonym"), 0.996, "symbol", "related_synonym"
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
                        f.array(f.col("id")), 1.0, "symbol", "id"
                    ).alias("identifier"),
                    annotate_entity(
                        f.col("dbXRefs"), 0.999, "symbol", "crossref"
                    ).alias("crossRefs"),
                    annotate_entity(
                        f.col("obsoleteXRefs"), 0.998, "symbol", "obsolete_crossref"
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
