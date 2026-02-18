"""Open Targets drug index."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from ontoma.dataset.raw_entity_lut import RawEntityLUT
from ontoma.common.utils import (
    annotate_entity,
    get_alternative_translations
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class OpenTargetsDrug:
    """Class to extract drug entities from a dataset following the Open Targets drug index schema."""

    @classmethod
    def as_label_lut(
        cls: type[OpenTargetsDrug], 
        drug_index: DataFrame
    ) -> RawEntityLUT:
        """Generate drug label lookup table from a dataset following the Open Targets drug index schema.
        
        Args:
            drug_index (DataFrame): Dataset following the Open Targets drug index schema.

        Returns:
            RawEntityLUT: Drug label lookup table.
        """
        return RawEntityLUT(
            _df=(
                drug_index
                # early filter: only process drugs with meaningful label information
                .filter(
                    (~f.lower(f.col("name")).startswith("chembl"))
                    | (f.size(f.col("tradeNames")) > 0)
                    | (f.size(f.col("synonyms")) > 0)
                )
                # filter crossReferences for sources that have labels
                .withColumn(
                    "crossReferences", 
                    f.filter(
                        f.col("crossReferences"),
                        lambda x: x["source"].isin("DailyMed", "USAN", "EMA")
                    )
                )
                # transform array of structs to array of strings and format ids
                .withColumn(
                    "crossReferences",
                    f.transform(
                        f.col("crossReferences"),
                        lambda x: f.when(
                            # if it's a DailyMed or USAN id, replace spaces encoded as "%20"
                            x["source"].isin("DailyMed", "USAN"),
                            f.transform(x["ids"], lambda i: f.regexp_replace(i, "%20", " "))
                        ).when(
                            # if it's an EMA id, extract the last part
                            x["source"] == "EMA",
                            f.transform(x["ids"], lambda i: f.regexp_extract(i, r'.+/EPAR/(.+)', 1))
                        ).otherwise(x["ids"])
                    )
                )
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
                        f.col("tradeNames"), 0.999, "term"
                    ).alias("tradeNamesTerm"),
                    annotate_entity(
                        f.col("tradeNames"), 0.999, "symbol"
                    ).alias("tradeNamesSymbol"),
                    annotate_entity(
                        f.col("synonyms"), 0.999, "term"
                    ).alias("synonymsTerm"),
                    annotate_entity(
                        f.col("synonyms"), 0.999, "symbol"
                    ).alias("synonymsSymbol"),
                    annotate_entity(
                        f.flatten(f.col("crossReferences")), 0.998, "term"
                    ).alias("crossReferencesTerm"),
                    annotate_entity(
                        f.flatten(f.col("crossReferences")), 0.998, "symbol"
                    ).alias("crossReferencesSymbol")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(
                                f.col("nameTerm"),
                                f.col("nameSymbol"),
                                f.col("tradeNamesTerm"),
                                f.col("tradeNamesSymbol"),
                                f.col("synonymsTerm"),
                                f.col("synonymsSymbol"),
                                f.col("crossReferencesTerm"),
                                f.col("crossReferencesSymbol")
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
                    f.lit("CD").alias("entityType"),
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
        cls: type[OpenTargetsDrug], 
        drug_index: DataFrame
    ) -> RawEntityLUT:
        """Generate drug id lookup table from the Open Targets drug index.

        Args:
            drug_index (DataFrame): Open Targets drug index.

        Returns:
            RawEntityLUT: Drug id lookup table.
        """
        return RawEntityLUT(
            _df=(
                drug_index
                # the drug index has cross references from six sources
                # sources that have ids (chEBI, drugbank) are processed below
                # sources that have labels (DailyMed, USAN, EMA) are processed in the as_label_lut function above
                # cross references from INN are excluded as they are not useful for drug ontology mapping
                # (they indicate which International Nonproprietary Names for Pharmaceutical Substances (INN) Proposed List the drug is mentioned in)
                
                # filter crossReferences for sources that have ids
                .withColumn(
                    "crossReferences", 
                    f.filter(
                        f.col("crossReferences"),
                        lambda x: x["source"].isin("chEBI", "drugbank")
                    )
                )
                # transform array of structs to array of strings and format ids
                .withColumn(
                    "crossReferences",
                    f.transform(
                        f.col("crossReferences"),
                        lambda x: f.when(
                            # if it's a chEBI id, append "CHEBI" as a prefix
                            x["source"] == "chEBI",
                            f.concat(f.lit("CHEBI"), x["ids"][0])
                        ).otherwise(x["ids"][0])
                    )
                )
                # extract entities from relevant fields and annotate entity with score and nlpPipelineTrack
                .select(
                    f.col("id").alias("entityId"),
                    annotate_entity(
                        f.col("crossReferences"), 1.0, "symbol"
                    ).alias("crossReferences")
                )
                # explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.col("crossReferences"),
                    )
                )
                # select relevant fields and specify entity type
                .select(
                    f.col("entityId"),
                    f.col("entity.entityLabel").alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.lit("CD").alias("entityType"),
                    f.lit("id").alias("entityKind")
                )
                # cleanup
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
