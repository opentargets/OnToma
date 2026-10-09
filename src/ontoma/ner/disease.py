"""Disease entity extraction using NER for OnToma preprocessing."""

from __future__ import annotations

from loguru import logger
from typing import TYPE_CHECKING

from ontoma.ner._extractors import attach_extractions, collect_distinct_texts
from ontoma.ner._pipelines import create_ner_pipeline

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

BIOBERT_LABELS = ["DISEASE"]


def extract_disease_entities(
    spark: SparkSession,
    df: DataFrame,
    input_col: str,
    output_col: str = "extracted_diseases",
) -> DataFrame:
    """Extract disease entities from raw indication labels using NER.

    This is a preprocessing step for OnToma mapping. Use this when your
    input text contains indications that need to be extracted and cleaned
    before mapping to disease IDs.

    Args:
        spark: Active Spark session
        df: Spark DataFrame with drug labels
        input_col: Column containing raw drug labels
        output_col: Column name for extracted entities (array of strings)

    Returns:
        Spark DataFrame with extracted entities column

    Note:
        - First run will download models (~430MB)
        - NER runs on the driver, once per distinct input text
        - On Apple Silicon, uses MPS acceleration automatically
    """
    if input_col not in df.columns:
        raise ValueError(f"Column '{input_col}' not found in DataFrame")

    logger.info("load biobert model...")
    biobert_pipeline = create_biobert_disease_ner()

    logger.info("collect distinct texts for ner processing...")
    texts = collect_distinct_texts(df, input_col)

    all_results = []
    for text in texts:
        if not text or text.strip() == "":
            all_results.append([])
            continue

        results = set()

        try:
            entities = biobert_pipeline(text)
            for ent in entities:
                entity_label = ent.get("entity_group", "").upper()
                if any(label in entity_label for label in ["DISEASE"]):
                    word = ent["word"].strip()
                    if len(word) > 1 and not word.isdigit():
                        results.add(word.lower())
        except Exception as e:
            print(f"ner failed for '{text}': {e}")

        all_results.append(sorted(list(results)))

    logger.info("join results back to Spark DataFrame...")
    result_df = attach_extractions(spark, df, input_col, output_col, texts, all_results)

    logger.info("disease entity extraction complete.")

    return result_df


def create_biobert_disease_ner():
    """Create BioBERT NER pipeline for disease extraction.

    BioBERT model fine-tuned in NER task with BC5CDR-diseases and NCBI-diseases corpus

    Returns:
        Transformers NER pipeline for BioBERT
    """
    return create_ner_pipeline(
        model_name="alvaroalon2/biobert_diseases_ner",
        tokenizer_name="alvaroalon2/biobert_diseases_ner",
        aggregation_strategy="max",
    )
