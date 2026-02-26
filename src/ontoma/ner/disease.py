"""Disease entity extraction using NER for OnToma preprocessing."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ontoma.ner._pipelines import create_ner_pipeline

from pyspark.sql.types import StructType, StructField, ArrayType, StringType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

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
        - Processing converts Spark DF → Pandas → NER → Spark DF
        - On Apple Silicon, uses MPS acceleration automatically
    """
    if input_col not in df.columns:
        raise ValueError(f"Column '{input_col}' not found in DataFrame")

    logger.info("load biobert model...")
    biobert_pipeline = create_biobert_disease_ner()

    logger.info("convert spark dataframe to pandas for ner processing...")
    pdf = df.toPandas()
    texts = pdf[input_col].fillna("").tolist()

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

    pdf[output_col] = all_results

    logger.info("convert results back to Spark DataFrame...")
    result_df = spark.createDataFrame(
        pdf,
        schema=StructType(
            df.schema.fields + [StructField(output_col, ArrayType(StringType()), True)]
        ),
    )

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
