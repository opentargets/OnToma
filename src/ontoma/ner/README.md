# OnToma NER Module

Named Entity Recognition (NER) preprocessing for OnToma entity mapping.

## Purpose

The NER module extracts clean entity names from raw text labels **before** mapping them to ontology IDs. This is useful when your input data contains:

### Drug Labels
- Dosages: "0.001% atropine" → "atropine"
- Drug forms: "aspirin tablets" → "aspirin"
- Combinations: "abacavir + lamivudine" → ["abacavir", "lamivudine"]
- Brand names: "Actemra" → "actemra"

### Disease Labels
- Clinical descriptions: "metastatic melanoma treatment" → "melanoma"
- Multiple conditions: "type 2 diabetes and hypertension" → ["diabetes", "hypertension"]
- Medical terminology: "acute myocardial infarction" → "myocardial infarction"

## Usage

### Drug Entity Extraction

```python
from pyspark.sql import SparkSession
from ontoma import OnToma, OpenTargetsDrug
from ontoma.ner.drug import extract_drug_entities
import pyspark.sql.functions as f

# Step 1: Extract clean drug entities from raw labels
df_extracted = extract_drug_entities(
    spark=spark,
    df=raw_drug_df,
    input_col="raw_drug_label",
    output_col="extracted_drugs"
)

# Result: raw_drug_label="0.001% atropine" → extracted_drugs=["atropine"]

# Step 2: Explode arrays for mapping
df_exploded = df_extracted.select(
    "*", 
    f.explode("extracted_drugs").alias("clean_drug_name")
)

# Step 3: Map to drug IDs using OnToma
drug_index = spark.read.parquet("path/to/drug/index")
drug_lut = OpenTargetsDrug.as_label_lut(drug_index)
ont = OnToma(spark=spark, entity_lut_list=[drug_lut])

mapped_df = ont.map_entities(
    df=df_exploded,
    result_col_name="drug_ids",
    entity_col_name="clean_drug_name",
    entity_kind="label",
    type_col=f.lit("CD")
)
```

### Disease Entity Extraction

```python
from pyspark.sql import SparkSession
from ontoma import OnToma, OpenTargetsDisease
from ontoma.ner.disease import extract_disease_entities
import pyspark.sql.functions as f

# Step 1: Extract clean disease entities from raw labels
df_extracted = extract_disease_entities(
    spark=spark,
    df=raw_disease_df,
    input_col="raw_indication_label",
    output_col="extracted_diseases"
)

# Result: raw_indication_label="metastatic melanoma treatment" → extracted_diseases=["melanoma"]

# Step 2: Explode arrays for mapping
df_exploded = df_extracted.select(
    "*", 
    f.explode("extracted_diseases").alias("clean_disease_name")
)

# Step 3: Map to disease IDs using OnToma
disease_index = spark.read.parquet("path/to/disease/index")
disease_lut = OpenTargetsDisease.as_label_lut(disease_index)
ont = OnToma(spark=spark, entity_lut_list=[disease_lut])

mapped_df = ont.map_entities(
    df=df_exploded,
    result_col_name="disease_ids",
    entity_col_name="clean_disease_name",
    entity_kind="label",
    type_col=f.lit("DS")
)
```

### Options

#### Drug Extraction
```python
extract_drug_entities(
    spark=spark,
    df=df,
    input_col="raw_label",
    output_col="extracted_drugs",
    use_regex=True,           # Regex for biologics (-mab, -tinib, etc.)
    use_biobert=True,         # BioBERT for precise extraction
    use_drugtemist=True,      # DrugTEMIST for brand names (fallback)
    batch_size=128,           # Increase for GPU, decrease for memory
    explode_results=False     # Set True to auto-explode arrays
)
```

#### Disease Extraction
```python
extract_disease_entities(
    spark=spark,
    df=df,
    input_col="raw_indication",
    output_col="extracted_diseases"
)
```

## Architecture

### Drug Extraction: Tiered Strategy

The drug module uses a **lazy tiered approach** for efficiency:

1. **Regex (optional)**: Pattern matching for biologics with common suffixes
   - `-mab` (monoclonal antibodies): rituximab, trastuzumab
   - `-tinib` (kinase inhibitors): imatinib, sunitinib
   - `-prazole` (PPIs), `-statin`, `-pril`, etc.

2. **BioBERT**: Precise entity extraction
   - Small molecules
   - Salt forms (e.g., "niraparib tosylate monohydrate")
   - Drug combinations (splits "drug A + drug B")

3. **DrugTEMIST** (lazy fallback): Only runs on texts where BioBERT found nothing
   - Brand names: "Actemra" → "actemra"
   - Clinical terminology
   - Broader coverage but less precise

### Disease Extraction: Single Model Strategy

The disease module uses a **simplified single-model approach**:

1. **BioBERT Disease Model**: Direct entity extraction
   - Clinical descriptions and medical terminology
   - Multiple conditions in single text
   - Fast processing without batching complexity


## Models

### Drug Models

#### BioBERT (`alvaroalon2/biobert_chemical_ner`)
- **Training**: BC5CDR chemical entity dataset
- **Strengths**: Small molecules, precise boundaries, salt forms
- **Weaknesses**: Misses some brand names and biologics
- **Size**: ~430MB

#### DrugTEMIST (`BSC-NLP4BIA/bsc-bio-ehr-es-carmen-drugtemist`)
- **Training**: Clinical EHR data (Spanish + English)
- **Strengths**: Brand names, clinical terminology, biologics
- **Weaknesses**: Less precise boundaries, sometimes over-extracts
- **Size**: ~500MB

### Disease Models

#### BioBERT Disease (`alvaroalon2/biobert_diseases_ner`)
- **Training**: BC5CDR-diseases and NCBI-diseases corpus
- **Strengths**: Medical terminology, clinical descriptions
- **Weaknesses**: May miss rare disease names
- **Size**: ~430MB

## Performance

For Apple Silicon (M1/M2/M3), PyTorch will automatically use MPS acceleration.

### Drug Extraction
#### Apple M1 Pro
- **175k drug labels**: ~10-15 minutes
- **Batch size**: 128-256 (optimal)
- **Acceleration**: MPS (Metal Performance Shaders)

#### CPU-only
- **175k drug labels**: ~60 minutes
- **Batch size**: 32-64 (optimal)

### Disease Extraction
#### Apple M1 Pro
- **1k disease labels**: 25 seconds
- **No batching required**: Streamlined processing
- **Acceleration**: MPS (Metal Performance Shaders)

#### CPU-only
- **1k disease labels**: ~35 seconds
- **No batching required**: Streamlined processing

## Entity Types

- ✅ **Drugs** (small molecules, biologics, brand names)
- ✅ **Diseases** (clinical descriptions, medical terminology)

