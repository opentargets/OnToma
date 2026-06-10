"""Open Targets drug index."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from ontoma.dataset.raw_entity_lut import RawEntityLUT
from ontoma.common.utils import (
    COMPONENT_OF_PATTERN,
    annotate_entity,
    extract_combination_product,
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
                # the drug index ships tradeNames / synonyms as {label, source}
                # structs (label = the name, source = provenance e.g. ChEMBL / AACT).
                # tradeNames are all ChEMBL-sourced; synonyms are split by source so
                # curated ChEMBL synonyms can be scored above LLM-mined AACT synonyms.
                .withColumn("tradeNames", f.transform(f.col("tradeNames"), lambda x: x["label"]))
                .withColumn(
                    "synonymsChembl",
                    f.transform(
                        f.filter(f.col("synonyms"), lambda s: s["source"] == "ChEMBL"),
                        lambda s: s["label"]
                    )
                )
                .withColumn(
                    "synonymsAact",
                    f.transform(
                        f.filter(f.col("synonyms"), lambda s: s["source"] == "AACT"),
                        lambda s: s["label"]
                    )
                )
                # early filter: only process drugs with meaningful label information
                .filter(
                    (~f.lower(f.col("name")).startswith("chembl"))
                    | (f.size(f.col("tradeNames")) > 0)
                    | (f.size(f.col("synonymsChembl")) > 0)
                    | (f.size(f.col("synonymsAact")) > 0)
                )
                # extract combination products encoded as "{molecule} component of {product}"
                # in the trade names and synonyms (e.g. "Ivacaftor component of symkevi"),
                # so the product (symkevi) maps to this component molecule's id
                .withColumn(
                    "combinationProducts",
                    f.array_distinct(
                        f.filter(
                            f.transform(
                                f.concat(
                                    f.coalesce(f.col("tradeNames"), f.array()),
                                    f.coalesce(f.col("synonymsChembl"), f.array()),
                                    f.coalesce(f.col("synonymsAact"), f.array())
                                ),
                                lambda x: extract_combination_product(x)
                            ),
                            lambda x: f.length(x) > 0
                        )
                    )
                )
                # the "{molecule} component of {product}" phrase itself is never a
                # useful query label, so drop it from the trade names and synonyms
                # (the extracted product replaces it)
                .withColumn(
                    "tradeNames",
                    f.filter(
                        f.coalesce(f.col("tradeNames"), f.array()),
                        lambda x: ~x.rlike(COMPONENT_OF_PATTERN)
                    )
                )
                .withColumn(
                    "synonymsChembl",
                    f.filter(
                        f.coalesce(f.col("synonymsChembl"), f.array()),
                        lambda x: ~x.rlike(COMPONENT_OF_PATTERN)
                    )
                )
                .withColumn(
                    "synonymsAact",
                    f.filter(
                        f.coalesce(f.col("synonymsAact"), f.array()),
                        lambda x: ~x.rlike(COMPONENT_OF_PATTERN)
                    )
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
                        f.array(f.col("name")), "tbd", 1.0, "name"
                    ).alias("name"),
                    annotate_entity(
                        f.col("tradeNames"), "tbd", 0.999, "trade_name"
                    ).alias("tradeNames"),
                    # curated ChEMBL synonyms outrank LLM-mined AACT synonyms, which in
                    # turn outrank crossref-derived labels (LUT keeps the top-score id
                    # per normalised label, so an AACT-only label still maps)
                    annotate_entity(
                        f.col("synonymsChembl"), "tbd", 0.999, "synonym"
                    ).alias("synonymsChembl"),
                    annotate_entity(
                        f.col("synonymsAact"), "tbd", 0.998, "synonym_aact"
                    ).alias("synonymsAact"),
                    annotate_entity(
                        f.col("combinationProducts"), "tbd", 0.999, "trade_name_component"
                    ).alias("combinationProducts"),
                    annotate_entity(
                        f.flatten(f.col("crossReferences")), "tbd", 0.997, "crossref"
                    ).alias("crossReferences")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(
                                f.col("name"),
                                f.col("tradeNames"),
                                f.col("synonymsChembl"),
                                f.col("synonymsAact"),
                                f.col("combinationProducts"),
                                f.col("crossReferences")
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
                        f.array(f.col("id")), "symbol", 1.0, "id"
                    ).alias("identifier"),
                    annotate_entity(
                        f.col("crossReferences"), "symbol", 0.999, "crossref"
                    ).alias("crossRefs")
                )
                # flatten and explode array of structs
                .withColumn(
                    "entity",
                    f.explode(
                        f.flatten(
                            f.array(    
                                f.col("identifier"),
                                f.col("crossRefs")
                            )
                        )
                    )
                )
                # select relevant fields and specify entity type
                .select(
                    f.col("entityId"),
                    f.col("entity.entityLabel").alias("entityLabel"),
                    f.col("entity.entityScore").alias("entityScore"),
                    f.col("entity.nlpPipelineTrack").alias("nlpPipelineTrack"),
                    f.col("entity.entitySource").alias("entitySource"),
                    f.lit("CD").alias("entityType"),
                    f.lit("id").alias("entityKind")
                )
                # cleanup
                .filter((f.col("entityLabel").isNotNull()) & (f.length("entityLabel") > 0))
                .distinct()
            ),
            _schema=RawEntityLUT.get_schema()
        )
