"""Disease curation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from src.ontoma.dataset.raw_entity_lut import RawEntityLUT
from src.ontoma.common.utils import (
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
        disease_index: DataFrame
    ) -> RawEntityLUT:
        """Generate disease label lookup table from a disease curation table.

        Args:
            disease_curation (DataFrame): Input disease curation table.
            disease_index (DataFrame): Open Targets disease index.
        
        Returns:
            RawEntityLUT: Disease label lookup table.
        """
        return RawEntityLUT(
            _df=(
                disease_curation
                # extract entities from relevant fields and annotate entity with score and nlpPipelineTrack
                .select(
                    f.regexp_extract(
                        f.col("SEMANTIC_TAG"), r'^http.+/(\w+_\w+)$', 1
                    ).alias("entityId"),
                    annotate_entity(
                        f.array(f.col("PROPERTY_VALUE")), 1.0, "term"
                    ).alias("curationTerm"),
                    annotate_entity(
                        f.array(f.col("PROPERTY_VALUE")), 1.0, "symbol"
                    ).alias("curationSymbol")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(
                                f.col("curationTerm"),
                                f.col("curationSymbol")
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
                            # remove prefixes from curated disease labels
                            clean_disease_label(f.trim(f.col("entity.entityLabel")))
                        )
                    ).alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.lit("DS").alias("entityType"),
                    f.lit("label").alias("entityKind")
                )
                # only retain entityIds that are in the disease index
                .join(
                    disease_index.select(f.col("id").alias("entityId")).distinct(),
                    on="entityId",
                    how="inner"
                )
                # cleanup
                .filter((f.col("entityId").isNotNull()) & (f.length("entityId") > 0))
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
