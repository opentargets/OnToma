"""Tests for Spark-NLP classpath detection used by ``OnToma.__post_init__``.

OnToma must validate that Spark-NLP is actually available on the Spark
classpath, not that a particular config string (``spark.jars.packages``) is
set. Jars can be loaded via ``spark.jars``, a custom image, or a pre-baked
classpath — none of which populate ``spark.jars.packages`` — so a config-string
check reports false negatives. py4j resolves a present JVM class to a
``JavaClass`` and an absent one to a ``JavaPackage``; the probe keys off that.
"""

from unittest.mock import MagicMock

import pytest
from py4j.java_gateway import JavaClass, JavaPackage

from ontoma.ontoma import OnToma


def _spark_with_entrypoint(jvm_obj):
    """Fake SparkSession whose JVM lookup of the Spark-NLP entrypoint class
    resolves to ``jvm_obj``."""
    spark = MagicMock()
    spark._jvm.com.johnsnowlabs.nlp.DocumentAssembler = jvm_obj
    return spark


def test_available_when_class_on_classpath():
    """Available when the JVM resolves the entrypoint to a JavaClass, even
    with ``spark.jars.packages`` unset (jars loaded via spark.jars / image)."""
    present = JavaClass("com.johnsnowlabs.nlp.DocumentAssembler", MagicMock())
    spark = _spark_with_entrypoint(present)

    assert OnToma._spark_nlp_available(spark) is True


def test_unavailable_when_class_missing():
    """A missing JVM class resolves to a JavaPackage: Spark-NLP not present."""
    missing = JavaPackage("com.johnsnowlabs.nlp.DocumentAssembler", MagicMock())
    spark = _spark_with_entrypoint(missing)

    assert OnToma._spark_nlp_available(spark) is False


def test_unavailable_when_jvm_access_raises():
    """Any failure probing the JVM is treated as Spark-NLP unavailable."""

    class BoomJVM:
        def __getattr__(self, name):
            raise RuntimeError("gateway not ready")

    spark = MagicMock()
    spark._jvm = BoomJVM()

    assert OnToma._spark_nlp_available(spark) is False


@pytest.mark.integration
def test_available_against_live_jvm(spark):
    """Against a real Spark session with Spark NLP loaded, the probe resolves
    the entrypoint class on the live JVM and reports it available."""
    assert OnToma._spark_nlp_available(spark) is True
