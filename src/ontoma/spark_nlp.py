"""Helpers for loading Spark NLP into a Spark session."""

from __future__ import annotations

import re
from importlib.metadata import version


def spark_nlp_coordinate(pyspark_version: str | None = None, spark_nlp_version: str | None = None) -> str:
    """Return the Spark NLP Maven coordinate matching a PySpark runtime.

    Spark 3 runs Scala 2.12 and Spark 4 runs Scala 2.13, and Spark NLP publishes
    a separate artifact for each. Plain Spark 4.0.0 needs the dedicated
    ``spark400`` artifact. Spark NLP supports Spark 4 from 7.0.0 onwards.

    Use the result as ``spark.jars.packages`` when building a session, e.g.
    ``SparkConf().set("spark.jars.packages", spark_nlp_coordinate())``.

    Args:
        pyspark_version (str | None): PySpark version. Defaults to the installed one.
        spark_nlp_version (str | None): Spark NLP version. Defaults to the installed one.

    Returns:
        str: Maven coordinate, e.g. ``com.johnsnowlabs.nlp:spark-nlp_2.12:6.1.5``.

    Raises:
        ValueError: If the PySpark version is not a Spark 3 or 4 release, or if
            Spark 4 is paired with a Spark NLP version older than 7.0.0.
    """
    pyspark_version = pyspark_version or version("pyspark")
    spark_nlp_version = spark_nlp_version or version("spark-nlp")

    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", pyspark_version)
    if match is None or match.group(1) not in ("3", "4"):
        raise ValueError(f"Unsupported PySpark version '{pyspark_version}': expected a Spark 3.x or 4.x release.")

    if match.group(1) == "3":
        return f"com.johnsnowlabs.nlp:spark-nlp_2.12:{spark_nlp_version}"

    if int(spark_nlp_version.split(".")[0]) < 7:
        raise ValueError(f"Spark NLP {spark_nlp_version} does not support Spark 4; use Spark NLP 7.0.0 or newer.")
    suffix = "-spark400" if pyspark_version == "4.0.0" else ""
    return f"com.johnsnowlabs.nlp:spark-nlp{suffix}_2.13:{spark_nlp_version}"
