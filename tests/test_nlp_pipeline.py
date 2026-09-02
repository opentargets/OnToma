"""Tests for NLPPipeline term normalisation."""

import pytest

from ontoma.nlp_pipeline import NLPPipeline


@pytest.mark.integration
def test_apply_pipeline_case_insensitive_single_letter_tokens(spark):
    """Labels differing only in the case of a single-letter token (e.g. "type
    I" vs "type i") must normalise to the same term tokens.

    Regression test: the stopword-cleaning stage used to be case-sensitive
    with only lowercase "a"/"i" in its stop-word list, so a lowercase "i"
    token was dropped while an uppercase "I" token survived, producing
    different token sets for what should match as the same label.
    """
    expected = {
        "Neurofibromatosis type I": ["neurofibromatosi", "typ", "i"],
        "neurofibromatosis type i": ["neurofibromatosi", "typ", "i"],
        "Cystinuria, type A": ["cystinuria", "typ", "a"],
        "cystinuria, type a": ["cystinuria", "typ", "a"],
    }
    df = spark.createDataFrame([(label,) for label in expected], ["label"])

    result_df = NLPPipeline.apply_pipeline(df, "label")
    result = {
        row["label"]: row["finished_term"]
        for row in result_df.select("label", "finished_term").collect()
    }

    for label, expected_tokens in expected.items():
        assert result[label] == expected_tokens, f"{label!r}: {result[label]}"
