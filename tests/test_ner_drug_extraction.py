"""Tests for drug entity extraction using NER."""

import pytest

from ontoma.ner.drug import extract_drug_entities


@pytest.mark.slow
def test_extract_drug_entities(spark):
    """Test drug entity extraction with BioBERT and DrugTEMIST.
    
    This test verifies that the NER pipeline correctly extracts drug entities
    from various raw drug labels, including:
    - Dosages and percentages (0.01% Atropine → atropine)
    - Drug forms (solution, wipes, coated)
    - Combinations (A + B → [A, B])
    - Brand names and generic names
    - Biologics (antibodies)
    - Salt forms (Tosylate Monohydrate)
    """
    # Test data with expected results
    test_data = [
        ("0.01% Atropine", ["atropine"]),
        ("131I-apamistmab", ["apamistmab"]),
        ("15 mg tolvaptan", ["tolvaptan"]),
        ("2% Chlorhexidine solution wipes", ["chlorhexidine"]),
        ("81 mg enteric coated aspirin", ["aspirin"]),
        ("clopidogrel napadisilate + aspirin", ["aspirin", "clopidogrel napadisilate"]),
        ("(B) zidovudine, lamivudine and efavirenz", ["efavirenz", "lamivudine", "zidovudine"]),
        ("Clopidogrel plus ASA", ["asa", "clopidogrel"]),
        ("possibly trastuzumab", ["trastuzumab"]),
        ("rituximab", ["rituximab"]),
        ("Niraparib Tosylate Monohydrate", ["niraparib tosylate monohydrate"]),
    ]
    
    df = spark.createDataFrame([(label,) for label, _ in test_data], ["raw_drug_label"])
    
    result_df = extract_drug_entities(
        spark=spark,
        df=df,
        input_col="raw_drug_label",
        output_col="extracted_drugs",
        use_regex=True,
        use_biobert=True,
        use_drugtemist=True,
        batch_size=128
    )
    
    result_pdf = result_df.toPandas()
    for i, (raw_label, expected_drugs) in enumerate(test_data):
        actual_drugs = result_pdf.iloc[i]["extracted_drugs"]
        
        # Sort both for comparison
        assert sorted(actual_drugs) == sorted(expected_drugs), (
            f"Failed for '{raw_label}': "
            f"expected {expected_drugs}, got {actual_drugs}"
        )


@pytest.mark.slow
def test_extract_drug_entities_biobert_only(spark):
    """Test extraction with BioBERT only (no DrugTEMIST)."""
    test_data = [
        ("aspirin 100mg", ["aspirin"]),
        ("rituximab", ["rituximab"]),  # BioBERT should catch this with regex
    ]
    
    df = spark.createDataFrame([(label,) for label, _ in test_data], ["raw_drug_label"])
    
    result_df = extract_drug_entities(
        spark=spark,
        df=df,
        input_col="raw_drug_label",
        output_col="extracted_drugs",
        use_regex=True,
        use_biobert=True,
        use_drugtemist=False,  # Disable DrugTEMIST
        batch_size=128
    )
    
    result_pdf = result_df.toPandas()
    for i, (raw_label, expected_drugs) in enumerate(test_data):
        actual_drugs = result_pdf.iloc[i]["extracted_drugs"]
        assert sorted(actual_drugs) == sorted(expected_drugs), (
            f"Failed for '{raw_label}': expected {expected_drugs}, got {actual_drugs}"
        )


def test_extract_drug_entities_invalid_config(spark):
    """Test that invalid configuration raises appropriate errors."""
    df = spark.createDataFrame([("aspirin",)], ["raw_drug_label"])
    
    # Both models disabled
    with pytest.raises(ValueError, match="At least one of use_biobert or use_drugtemist"):
        extract_drug_entities(
            spark=spark,
            df=df,
            input_col="raw_drug_label",
            use_biobert=False,
            use_drugtemist=False
        )
    
    # Invalid column name
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        extract_drug_entities(
            spark=spark,
            df=df,
            input_col="nonexistent",
            use_biobert=True,
            use_drugtemist=False
        )
