"""Tests for disease entity extraction using NER."""

import pytest

from ontoma.ner import disease as disease_module


@pytest.mark.slow
def test_extract_disease_entities_basic(spark, monkeypatch):
    """Ensure disease extraction produces cleaned lower-case entities."""
    test_data = [
        ("Metastatic melanoma treatment", ["melanoma"]),
        ("Type 2 diabetes and resistant hypertension", ["diabetes", "hypertension"]),
        ("unknown condition", []),
    ]

    # Mock the pipeline with simple function returning expected entities
    def mock_pipeline(text):
        responses = {
            "Metastatic melanoma treatment": [
                {"entity_group": "DISEASE", "word": "Melanoma"},
            ],
            "Type 2 diabetes and resistant hypertension": [
                {"entity_group": "DISEASE", "word": "Diabetes"},
                {"entity_group": "gene", "word": "TP53"},
                {"entity_group": "DISEASE", "word": "hypertension"},
            ],
            "unknown condition": [],
        }
        return responses.get(text, [])

    monkeypatch.setattr(disease_module, "create_biobert_disease_ner", lambda: mock_pipeline)

    df = spark.createDataFrame([(text,) for text, _ in test_data], ["raw_indication"])

    result_df = disease_module.extract_disease_entities(
        spark=spark,
        df=df,
        input_col="raw_indication",
        output_col="disease_entities",
    )

    result_pdf = result_df.toPandas()
    for i, (raw_text, expected_entities) in enumerate(test_data):
        actual_entities = result_pdf.iloc[i]["disease_entities"]
        assert sorted(actual_entities) == sorted(expected_entities), (
            f"Failed for '{raw_text}': "
            f"expected {expected_entities}, got {actual_entities}"
        )


def test_extract_disease_entities_skips_blank_texts(spark, monkeypatch):
    """Blank or missing indications should produce empty extractions."""
    test_data = [
        ("", []),
        ("   ", []),
        (None, []),
        ("Rare syndrome", ["syndrome"]),
    ]

    # Track calls with a simple list
    calls = []
    
    def mock_pipeline(text):
        calls.append(text)
        responses = {
            "Rare syndrome": [
                {"entity_group": "DISEASE", "word": "Syndrome"},
            ],
        }
        return responses.get(text, [])

    monkeypatch.setattr(disease_module, "create_biobert_disease_ner", lambda: mock_pipeline)

    df = spark.createDataFrame([(text,) for text, _ in test_data], ["raw_indication"])

    result_df = disease_module.extract_disease_entities(
        spark=spark,
        df=df,
        input_col="raw_indication",
        output_col="disease_entities",
    )

    result_pdf = result_df.toPandas()
    for i, (raw_text, expected_entities) in enumerate(test_data):
        # toPandas() returns array columns as lists before Spark 4.2 and as numpy arrays from 4.2
        actual_entities = list(result_pdf.iloc[i]["disease_entities"])
        assert actual_entities == expected_entities, (
            f"Failed for '{raw_text}': "
            f"expected {expected_entities}, got {actual_entities}"
        )

    # Pipeline should only run for non-empty inputs
    assert calls == ["Rare syndrome"]


def test_extract_disease_entities_preserves_other_columns(spark_arrow_off, monkeypatch):
    """Nulls and types in passed-through columns survive extraction unchanged."""
    monkeypatch.setattr(
        disease_module,
        "create_biobert_disease_ner",
        lambda: lambda text: [{"entity_group": "DISEASE", "word": "Syndrome"}],
    )

    df = spark_arrow_off.createDataFrame(
        [
            (1, "Rare syndrome", None, None),
            (2, None, "note", 7),
            (3, "Rare syndrome", "other", None),
        ],
        "id int, raw_indication string, note string, count int",
    )

    result_df = disease_module.extract_disease_entities(
        spark=spark_arrow_off,
        df=df,
        input_col="raw_indication",
        output_col="disease_entities",
    )

    assert result_df.columns == ["id", "raw_indication", "note", "count", "disease_entities"]
    assert {row.id: row.asDict() for row in result_df.collect()} == {
        1: {"id": 1, "raw_indication": "Rare syndrome", "note": None, "count": None, "disease_entities": ["syndrome"]},
        2: {"id": 2, "raw_indication": None, "note": "note", "count": 7, "disease_entities": []},
        3: {"id": 3, "raw_indication": "Rare syndrome", "note": "other", "count": None, "disease_entities": ["syndrome"]},
    }


def test_extract_disease_entities_invalid_column(spark):
    """Invalid input columns should raise ValueError."""
    df = spark.createDataFrame(
        [("Metastatic melanoma treatment",)],
        ["raw_indication"],
    )

    with pytest.raises(ValueError, match="Column 'missing' not found"):
        disease_module.extract_disease_entities(
            spark=spark,
            df=df,
            input_col="missing",
        )
