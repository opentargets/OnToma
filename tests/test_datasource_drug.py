"""Tests for the Open Targets drug datasource extraction."""

import pytest
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from ontoma.datasource.drug import OpenTargetsDrug


_LABEL_SOURCE = ArrayType(
    StructType(
        [
            StructField("label", StringType()),
            StructField("source", StringType()),
        ]
    )
)

DRUG_INDEX_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("tradeNames", _LABEL_SOURCE),
        StructField("synonyms", _LABEL_SOURCE),
        StructField(
            "crossReferences",
            ArrayType(
                StructType(
                    [
                        StructField("source", StringType()),
                        StructField("ids", ArrayType(StringType())),
                    ]
                )
            ),
        ),
    ]
)


def _ls(*names):
    """Build a [{label, source}] struct list (all ChEMBL-sourced) for fixtures."""
    return [(n, "ChEMBL") for n in names]


@pytest.fixture(scope="module")
def drug_label_rows(spark):
    """Collected label-LUT rows from a small in-memory drug index.

    Covers a combination product shared by two molecules (Symkevi =
    Ivacaftor + Tezacaftor), a plain trade name, and a noisy product name.
    tradeNames / synonyms follow the {label, source} struct schema.
    """
    data = [
        # Ivacaftor: combination product (symkevi) + plain trade name (Kalydeco),
        # a curated ChEMBL synonym (VX-770) + a mined AACT synonym (ivx-mined),
        # and a DailyMed crossref label (to exercise the score tiers)
        (
            "CHEMBL2010601",
            "IVACAFTOR",
            _ls("Ivacaftor component of symkevi", "Kalydeco"),
            _ls("Ivacaftor", "VX-770") + [("ivx-mined", "AACT")],
            [("DailyMed", ["ivacaftor-dm"])],
        ),
        # Tezacaftor: the other component of the same combination product
        (
            "CHEMBL3544914",
            "TEZACAFTOR",
            _ls("Tezacaftor component of symkevi"),
            [],
            [],
        ),
        # Noisy product name with leading punctuation to exercise cleanup
        (
            "CHEMBLNOISE",
            "SOMEDRUG",
            _ls("Somedrug component of /weirdname"),
            [],
            [],
        ),
        # Uppercase ChEMBL encoding, pattern carried in synonyms (not tradeNames)
        (
            "CHEMBLUPPER",
            "MODAFINIL",
            [],
            _ls("MODAFINIL COMPONENT OF THN102"),
            [],
        ),
        # Same product encoded in both tradeNames and synonyms -> deduped
        (
            "CHEMBLDUP",
            "SOMEDRUGTWO",
            _ls("Somedrugtwo component of dupprod"),
            _ls("Somedrugtwo component of dupprod"),
            [],
        ),
    ]
    drug_index = spark.createDataFrame(data, schema=DRUG_INDEX_SCHEMA)
    result = OpenTargetsDrug.as_label_lut(drug_index)
    return [row.asDict() for row in result.df.collect()]


def _labels(rows, entity_id):
    return {r["entityLabel"] for r in rows if r["entityId"] == entity_id}


def test_combination_product_maps_to_component_molecules(drug_label_rows):
    """Each component molecule of a combination gets the product as a label."""
    ivacaftor_rows = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBL2010601" and r["entityLabel"] == "symkevi"
    ]
    tezacaftor_rows = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBL3544914" and r["entityLabel"] == "symkevi"
    ]

    assert len(ivacaftor_rows) == 1
    assert len(tezacaftor_rows) == 1
    for row in ivacaftor_rows + tezacaftor_rows:
        assert row["entitySource"] == "trade_name_component"
        assert row["entityType"] == "CD"
        assert row["entityKind"] == "label"


def test_full_component_phrase_is_not_emitted(drug_label_rows):
    """The raw 'X component of Y' phrase is replaced, not kept as a label."""
    all_labels = {r["entityLabel"].lower() for r in drug_label_rows}
    assert "ivacaftor component of symkevi" not in all_labels
    assert "tezacaftor component of symkevi" not in all_labels


def test_plain_trade_name_is_preserved(drug_label_rows):
    """Plain trade names alongside component phrases still survive."""
    kalydeco = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBL2010601" and r["entityLabel"] == "Kalydeco"
    ]
    assert len(kalydeco) == 1
    assert kalydeco[0]["entitySource"] == "trade_name"


def test_noisy_product_name_is_cleaned(drug_label_rows):
    """Leading punctuation on extracted products is stripped."""
    labels = _labels(drug_label_rows, "CHEMBLNOISE")
    assert "weirdname" in labels
    assert "/weirdname" not in labels


def test_component_pattern_extracted_from_synonyms(drug_label_rows):
    """The pattern is also parsed from synonyms, not only trade names."""
    rows = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBLUPPER" and r["entityLabel"] == "THN102"
    ]
    assert len(rows) == 1
    assert rows[0]["entitySource"] == "trade_name_component"


def test_component_pattern_is_case_insensitive(drug_label_rows):
    """Uppercase 'COMPONENT OF' encoding is recognised and the phrase dropped."""
    all_labels = {r["entityLabel"].lower() for r in drug_label_rows}
    assert "modafinil component of thn102" not in all_labels


def test_product_shared_across_fields_is_deduplicated(drug_label_rows):
    """A product encoded in both trade names and synonyms yields one label row."""
    rows = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBLDUP" and r["entityLabel"] == "dupprod"
    ]
    assert len(rows) == 1


def test_synonym_score_tiers_by_source(drug_label_rows):
    """ChEMBL synonyms (0.999) outrank AACT synonyms (0.998), which outrank crossrefs (0.997)."""
    chembl_syn = [r for r in drug_label_rows if r["entitySource"] == "synonym"]
    aact_syn = [r for r in drug_label_rows if r["entitySource"] == "synonym_aact"]
    crossref = [r for r in drug_label_rows if r["entitySource"] == "crossref"]

    assert chembl_syn and aact_syn and crossref
    assert all(r["entityScore"] == 0.999 for r in chembl_syn)
    assert all(r["entityScore"] == 0.998 for r in aact_syn)
    assert all(r["entityScore"] == 0.997 for r in crossref)


def test_aact_synonym_label_present_with_source(drug_label_rows):
    """The mined AACT synonym maps to its molecule, tagged with the AACT source."""
    rows = [
        r
        for r in drug_label_rows
        if r["entityId"] == "CHEMBL2010601" and r["entityLabel"] == "ivx-mined"
    ]
    assert len(rows) == 1
    assert rows[0]["entitySource"] == "synonym_aact"
