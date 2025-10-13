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

OnToma requires OpenJDK 17 or higher to be installed on your system, as it's a prerequisite for Apache Spark 4.0.0.

#### macOS Installation

Install OpenJDK 17 using Homebrew:

```bash
brew install openjdk@17
```

After installation, you need to set the `JAVA_HOME` environment variable. Add the following to your shell configuration file (e.g., `~/.zshrc` or `~/.bash_profile`):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload your shell configuration:

```bash
source ~/.zshrc  # or source ~/.bash_profile
```

Verify the installation:

```bash
java -version
```

## Installation

```bash
uv pip install "git+https://github.com/opentargets/OnToma@vh-new-ontoma"
```

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
