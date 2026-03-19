"""Pytest configuration for OnToma tests."""

import pytest
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing.
    
    This session is created once per test session and reused across all tests.
    It includes the Spark NLP dependency required by OnToma.
    """
    config = (
        SparkConf()
        .set("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:6.1.3")
        .set("spark.driver.memory", "4g")
        .set("spark.sql.shuffle.partitions", "2")  # Reduce for faster tests
    )
    
    spark = SparkSession.builder.config(conf=config).master("local[2]").getOrCreate()
    
    yield spark
    
    spark.stop()
