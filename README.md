# OnToma

## Introduction

OnToma is a Python package for mapping entities to identifiers using lookup tables. It is optimised for large-scale entity mapping, and is designed to work with PySpark DataFrames.

OnToma supports the mapping of two kinds of entities: labels (e.g. `brachydactyly`) and ids (e.g. `OMIM:112500`).

OnToma currently has modules to generate lookup tables from the following datasources:
- Open Targets disease, target, and drug indices
- Disease curation tables with the `SEMANTIC_TAG` and `PROPERTY_VALUE` fields

The package features entity normalisation using [Spark NLP](https://sparknlp.org/), where entities in both the lookup table and the input dataframe are normalised to improve entity matching.

Successfully mapped entities may be mapped to multiple identifiers.

## Prerequisites

### Java Runtime Environment

OnToma requires OpenJDK 8 or 11 to be installed on your system, as it's a prerequisite for PySpark and Spark-NLP.

#### macOS Installation

Install OpenJDK 8 or 11 using Homebrew:

```bash
brew install openjdk@11
```

After installation, you need to set the `JAVA_HOME` environment variable. Add the following to your shell configuration file (e.g., `~/.zshrc` or `~/.bash_profile`):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload your shell configuration:

```bash
source ~/.zshrc
```

Verify the installation:

```bash
java -version
```

## Installation

```bash
pip install ontoma
```

### Optional: NER for Entity Extraction

OnToma includes an optional NER (Named Entity Recognition) module for extracting clean entity names from raw text labels. This is useful when your data contains dosages, drug forms, or other text that needs preprocessing.

To use NER features, see [NER Module Documentation](src/ontoma/ner/README.md).

## Spark session configuration

OnToma requires a Spark session configured to include the Spark NLP library.

```python
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# add Spark NLP library to Spark configuration
config = (
    SparkConf()
    .set("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:6.1.3")
)

# create Spark session
spark = SparkSession.builder.config(conf=config).getOrCreate()
```

## Usage example

Here is an example showing how OnToma can be used to map diseases:

First, load data to generate a disease label lookup table:

```python
from ontoma import OnToma
from ontoma import OpenTargetsDisease

disease_index = spark.read.parquet("path/to/disease/index")
disease_label_lut = OpenTargetsDisease.as_label_lut(disease_index)
```

Then, create the OnToma object to be used for mapping entities:

```python
ont = OnToma(
    spark = spark, 
    entity_lut_list = [disease_label_lut]
)
```

Given an input PySpark DataFrame `disease_df` containing the diseases to be mapped in the column `disease_name`:

```python
mapped_disease_df = ont.map_entities(
    df = disease_df,
    result_col_name = "mapped_ids",
    entity_col_name = "disease_name",
    entity_kind = "label",
    type_col = f.lit("DS")
)
```

## Using NER for preprocessing (drugs)

When your drug labels contain dosages, forms, or brand names, use the NER module to extract clean entity names before mapping:

```python
from ontoma.ner.drug import extract_drug_entities
import pyspark.sql.functions as f

# Extract clean drug entities from raw labels
df_extracted = extract_drug_entities(
    spark=spark,
    df=raw_drug_df,
    input_col="raw_drug_label",
    output_col="extracted_drugs"
)

# Explode arrays for mapping
df_exploded = df_extracted.select("*", f.explode("extracted_drugs").alias("clean_drug"))

# Map with OnToma
mapped_df = ont.map_entities(
    df=df_exploded,
    entity_col_name="clean_drug",
    entity_kind="label",
    type_col=f.lit("drug")
)
```

See [NER Module Documentation](src/ontoma/ner/README.md) for more details.

## Speeding up subsequent OnToma usage

PySpark uses lazy evaluation, meaning transformations are not executed until an action is triggered. 

When using the same OnToma object multiple times, it is recommended to specify a cache directory when creating the OnToma object using the `cache_dir` parameter to avoid re-running the lookup table processing logic on each use.

```python
ont = OnToma(
    spark = spark, 
    entity_lut_list = [disease_label_lut],
    cache_dir = "path/to/cache/dir"
)
```

## Development

### Running Tests

Install development dependencies:

```bash
uv sync --dev
```

Run all tests:

```bash
uv run pytest
```

Skip slow tests (e.g., NER tests that download large models):

```bash
uv run pytest -m "not slow"
```

See [tests/README.md](tests/README.md) for more details.
