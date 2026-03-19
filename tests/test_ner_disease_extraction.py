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
        actual_entities = result_pdf.iloc[i]["disease_entities"]
        assert actual_entities == expected_entities, (
            f"Failed for '{raw_text}': "
            f"expected {expected_entities}, got {actual_entities}"
        )

    # Pipeline should only run for non-empty inputs
    assert calls == ["Rare syndrome"]


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
