"""Tests for choosing the Spark NLP artifact that matches the PySpark runtime.

Spark 3 runs Scala 2.12 and Spark 4 runs Scala 2.13, and Spark NLP publishes a
separate artifact for each, so the coordinate depends on the PySpark version.
"""

import pytest

from ontoma.spark_nlp import spark_nlp_coordinate


@pytest.mark.parametrize(
    ("pyspark_version", "spark_nlp_version", "expected"),
    [
        ("3.5.7", "6.1.5", "com.johnsnowlabs.nlp:spark-nlp_2.12:6.1.5"),
        ("3.5.9", "7.0.0", "com.johnsnowlabs.nlp:spark-nlp_2.12:7.0.0"),
        ("4.0.0", "7.0.0", "com.johnsnowlabs.nlp:spark-nlp-spark400_2.13:7.0.0"),
        ("4.0.1", "7.0.0", "com.johnsnowlabs.nlp:spark-nlp_2.13:7.0.0"),
        ("4.1.3", "7.0.0", "com.johnsnowlabs.nlp:spark-nlp_2.13:7.0.0"),
    ],
)
def test_coordinate_follows_spark_major(pyspark_version, spark_nlp_version, expected):
    """Spark 3 gets the Scala 2.12 artifact, Spark 4 the Scala 2.13 one."""
    assert spark_nlp_coordinate(pyspark_version, spark_nlp_version) == expected


def test_spark_4_rejects_spark_nlp_6():
    """Spark NLP 6.x publishes no Spark 4 build; fail instead of loading a Spark 3 jar."""
    with pytest.raises(ValueError, match="does not support Spark 4"):
        spark_nlp_coordinate("4.1.3", "6.4.2")


@pytest.mark.parametrize("pyspark_version", ["2.4.8", "5.0.0", "4.1.0.dev1"])
def test_unsupported_pyspark_version(pyspark_version):
    """Anything other than a Spark 3.x or 4.x release is rejected."""
    with pytest.raises(ValueError, match="Unsupported PySpark version"):
        spark_nlp_coordinate(pyspark_version, "7.0.0")


def test_defaults_to_installed_versions():
    """Without arguments, the installed pyspark and spark-nlp versions are used."""
    from importlib.metadata import version

    assert spark_nlp_coordinate() == spark_nlp_coordinate(version("pyspark"), version("spark-nlp"))
