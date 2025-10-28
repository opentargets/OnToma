# OnToma NER Module

Named Entity Recognition (NER) preprocessing for OnToma entity mapping.

## Purpose

The NER module extracts clean entity names from raw text labels **before** mapping them to ontology IDs. This is useful when your input data contains:

- Dosages: "0.001% atropine" → "atropine"
- Drug forms: "aspirin tablets" → "aspirin"
- Combinations: "abacavir + lamivudine" → ["abacavir", "lamivudine"]
- Brand names: "Actemra" → "actemra"

## Usage

### Basic Example

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
    type_col=f.lit("drug")
)
```

### Options

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

## Architecture

### Tiered Extraction Strategy

The module uses a **lazy tiered approach** for efficiency:

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


## Models

### BioBERT (`alvaroalon2/biobert_chemical_ner`)
- **Training**: BC5CDR chemical entity dataset
- **Strengths**: Small molecules, precise boundaries, salt forms
- **Weaknesses**: Misses some brand names and biologics
- **Size**: ~430MB

### DrugTEMIST (`BSC-NLP4BIA/bsc-bio-ehr-es-carmen-drugtemist`)
- **Training**: Clinical EHR data (Spanish + English)
- **Strengths**: Brand names, clinical terminology, biologics
- **Weaknesses**: Less precise boundaries, sometimes over-extracts
- **Size**: ~500MB

## Performance

For Apple Silicon (M1/M2/M3), PyTorch will automatically use MPS acceleration.

### Apple M1 Pro
- **175k drug labels**: ~10-15 minutes
- **Batch size**: 128-256 (optimal)
- **Acceleration**: MPS (Metal Performance Shaders)

### CPU-only
- **175k drug labels**: ~60 minutes
- **Batch size**: 32-64 (optimal)

### GPU (CUDA)
- **175k drug labels**: ~20 minutes
- **Batch size**: 128-512 (optimal)

## Entity Types

- ✅ **Drugs** (small molecules, biologics, brand names)
Coming soon:
- 🚧 **Diseases** (planned)

