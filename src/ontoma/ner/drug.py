"""Drug entity extraction using NER for OnToma preprocessing."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

import pyspark.sql.functions as f

from ontoma.ner._extractors import extract_with_regex, extract_entities_from_batch
from ontoma.ner._pipelines import create_ner_pipeline

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# Biologic and common drug suffix patterns
DRUG_NAME_PATTERNS = [
    r'\w*mab\b',           # Monoclonal antibodies
    r'\w*cept\b',          # Receptor constructs
    r'\w*tinib\b',         # Kinase inhibitors
    r'\w*nib\b',           # TKIs
    r'\w*ciclib\b',        # CDK inhibitors
    r'\w*tidine\b',        # H2 antagonists
    r'\w*prazole\b',       # PPIs
    r'\w*statin\b',        # Statins
    r'\w*olol\b',          # Beta blockers
    r'\w*pril\b',          # ACE inhibitors
    r'\w*sartan\b',        # ARBs
    r'\w*floxacin\b',      # Fluoroquinolones
    r'\w*cillin\b',        # Penicillins
    r'\w*mycin\b',         # Macrolides
    r'\w*cycline\b',       # Tetracyclines
]

# Entity label filters for each model
BIOBERT_LABELS = ['CHEMICAL', 'DRUG', 'MEDICATION', 'CHEM']
DRUGTEMIST_LABELS = ['CHEMICAL', 'DRUG', 'MEDICATION', 'CHEM', 'FARMACO', 'NORMALIZABLES']


def create_biobert_drug_ner():
    """Create BioBERT NER pipeline for chemical/drug extraction.

    Uses BioBERT trained on BC5CDR dataset. Excellent for small molecules
    and precise entity extraction (including salt forms).

    Returns:
        Transformers NER pipeline for BioBERT
    """
    return create_ner_pipeline(
        model_name="alvaroalon2/biobert_chemical_ner",
        tokenizer_name="dmis-lab/biobert-base-cased-v1.1",
        aggregation_strategy="max"
    )


def create_drugtemist_drug_ner():
    """Create DrugTEMIST NER pipeline for drug extraction.

    Trained on clinical data. Better coverage of brand names and biologics
    compared to BioBERT, but less precise for chemical names.

    Returns:
        Transformers NER pipeline for DrugTEMIST
    """
    return create_ner_pipeline(
        model_name="BSC-NLP4BIA/bsc-bio-ehr-es-carmen-drugtemist",
        aggregation_strategy="max"
    )


def _process_batch_tiered(
    texts: List[str],
    biobert_pipeline,
    drugtemist_pipeline,
    use_regex: bool,
    batch_size: int
) -> List[List[str]]:
    """Process texts using tiered extraction strategy.
    
    Tiered approach:
    1. (Optional) Regex for biologics
    2. BioBERT for precise extraction
    3. DrugTEMIST only for texts where BioBERT found nothing
    
    Args:
        texts: List of drug label texts
        biobert_pipeline: BioBERT NER pipeline
        drugtemist_pipeline: DrugTEMIST NER pipeline (can be None)
        use_regex: Whether to use regex patterns
        batch_size: Batch size for NER processing
        
    Returns:
        List of lists, one per input text, containing extracted drug names
    """
    all_results = []
    texts_needing_drugtemist = []
    drugtemist_indices = []
    
    # Process in batches
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch_start_idx = i
        
        # Step 1: Regex (optional)
        if use_regex:
            regex_results = [
                extract_with_regex(text, DRUG_NAME_PATTERNS) if text else set() 
                for text in batch
            ]
        else:
            regex_results = [set() for _ in batch]
        
        # Step 2: BioBERT
        biobert_results = extract_entities_from_batch(batch, biobert_pipeline, BIOBERT_LABELS)
        
        # Combine and identify texts needing DrugTEMIST
        for idx, (regex_drugs, biobert_drugs, text) in enumerate(zip(regex_results, biobert_results, batch)):
            combined = regex_drugs | biobert_drugs
            
            if not combined and text and text.strip() and drugtemist_pipeline is not None:
                # No results yet - queue for DrugTEMIST
                texts_needing_drugtemist.append(text)
                drugtemist_indices.append(batch_start_idx + idx)
                all_results.append(None)  # Placeholder
            else:
                all_results.append(sorted(list(combined)))
        
        if (i + batch_size) % 5000 == 0:
            logger.info(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} with BioBERT...")
    
    # Step 3: DrugTEMIST for failed cases only
    if texts_needing_drugtemist and drugtemist_pipeline is not None:
        logger.info(f"Running DrugTEMIST on {len(texts_needing_drugtemist)} texts that BioBERT missed...")
        
        for i in range(0, len(texts_needing_drugtemist), batch_size):
            batch = texts_needing_drugtemist[i:i + batch_size]
            batch_indices = drugtemist_indices[i:i + batch_size]
            
            drugtemist_results = extract_entities_from_batch(batch, drugtemist_pipeline, DRUGTEMIST_LABELS)
            
            for original_idx, drugs in zip(batch_indices, drugtemist_results):
                all_results[original_idx] = sorted(list(drugs))
    
    # Fill any remaining Nones with empty lists
    all_results = [r if r is not None else [] for r in all_results]
    
    return all_results


def extract_drug_entities(
    spark: SparkSession,
    df: DataFrame,
    input_col: str,
    output_col: str = "extracted_drugs",
    use_regex: bool = True,
    use_biobert: bool = True,
    use_drugtemist: bool = True,
    batch_size: int = 128,
) -> DataFrame:
    """Extract drug entities from raw drug labels using NER.
    
    This is a preprocessing step for OnToma mapping. Use this when your
    drug labels contain dosages, forms, or other text that needs to be
    cleaned before mapping to drug IDs.
    
    Tiered extraction strategy:
    1. (Optional) Regex patterns for biologics (-mab, -tinib, etc.)
    2. BioBERT for precise entity extraction (small molecules, salt forms)
    3. DrugTEMIST as fallback for brand names and entities BioBERT misses
    
    Args:
        spark: Active Spark session
        df: Spark DataFrame with drug labels
        input_col: Column containing raw drug labels
        output_col: Column name for extracted entities (array of strings)
        use_regex: Extract biologics using regex patterns (default: True)
        use_biobert: Use BioBERT for precise entity extraction (default: True)
        use_drugtemist: Use DrugTEMIST as fallback (default: True)
        batch_size: Batch size for NER processing (default: 128)
        
    Returns:
        Spark DataFrame with extracted entities column
        
    Raises:
        ImportError: If torch or transformers are not installed
        ValueError: If neither use_biobert nor use_drugtemist is True
        
    Example:
        >>> from ontoma.ner.drug import extract_drug_entities
        >>> import pyspark.sql.functions as f
        >>> 
        >>> # Extract entities (returns arrays)
        >>> df = extract_drug_entities(
        ...     spark=spark,
        ...     df=drug_df,
        ...     input_col="raw_drug_label",
        ...     output_col="extracted_drugs"
        ... )
        >>> 
        >>> # Explode for mapping
        >>> df_exploded = df.select("*", f.explode("extracted_drugs").alias("drug"))
        >>> 
        >>> # Map with OnToma
        >>> mapped = ont.map_entities(
        ...     df=df_exploded,
        ...     entity_col_name="drug",
        ...     entity_kind="label",
        ...     type_col=f.lit("drug")
        ... )
        
    Note:
        - First run will download models (~430MB for BioBERT, ~480MB for DrugTEMIST)
        - Processing converts Spark DF → Pandas → NER → Spark DF
        - On Apple Silicon, uses MPS acceleration automatically
        - DrugTEMIST only runs on texts where BioBERT finds nothing (efficient)
    """
    if not use_biobert and not use_drugtemist:
        raise ValueError("At least one of use_biobert or use_drugtemist must be True")
    
    if not use_biobert:
        logger.warning("BioBERT is disabled. This may result in less precise extraction.")
    
    if input_col not in df.columns:
        raise ValueError(f"Column '{input_col}' not found in DataFrame")
    
    biobert_pipeline = None
    drugtemist_pipeline = None
    if use_biobert:
        logger.info("load biobert model...")
        biobert_pipeline = create_biobert_drug_ner()
    if use_drugtemist:
        logger.info("load drugtemist model...")
        drugtemist_pipeline = create_drugtemist_drug_ner()
    
    logger.info("convert spark dataframe to pandas for NER processing...")
    pdf = df.toPandas()
    
    logger.info(f"extract drug entities from {len(pdf)} rows (batch_size={batch_size})...")
    drug_texts = pdf[input_col].fillna("").tolist()
    
    extracted = _process_batch_tiered(
        texts=drug_texts,
        biobert_pipeline=biobert_pipeline,
        drugtemist_pipeline=drugtemist_pipeline,
        use_regex=use_regex,
        batch_size=batch_size
    )
    
    pdf[output_col] = extracted
    
    logger.info("convert results back to Spark DataFrame...")
    result_df = spark.createDataFrame(pdf)
    
    logger.info("drug entity extraction complete.")
    
    return result_df
