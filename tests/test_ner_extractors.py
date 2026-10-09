"""Tests for collecting NER texts and joining extractions back."""

from pyspark.sql.types import ArrayType, StringType

from ontoma.ner._extractors import attach_extractions, collect_distinct_texts


def _attach_upper(spark, df, input_col, output_col="out"):
    texts = collect_distinct_texts(df, input_col)
    return attach_extractions(spark, df, input_col, output_col, texts, [[t.upper()] for t in texts])


def test_collect_distinct_texts_dedups_and_sorts(spark):
    df = spark.createDataFrame([("b",), ("a",), (None,), ("b",), ("",)], "txt string")

    assert collect_distinct_texts(df, "txt") == ["", "a", "b"]


def test_attach_extractions_keeps_every_row_once(spark):
    df = spark.createDataFrame(
        [(1, "a"), (2, "a"), (3, None), (4, "b"), (5, "")], "id int, txt string"
    )

    result = _attach_upper(spark, df, "txt")

    assert sorted((r.id, r.out) for r in result.collect()) == [
        (1, ["A"]),
        (2, ["A"]),
        (3, []),
        (4, ["B"]),
        (5, [""]),
    ]


def test_attach_extractions_output_column_is_nullable(spark):
    df = spark.createDataFrame([("a",)], "txt string")

    field = _attach_upper(spark, df, "txt").schema["out"]

    assert field.dataType == ArrayType(StringType())
    assert field.nullable


def test_attach_extractions_empty_and_all_null_frames(spark):
    empty = spark.createDataFrame([], "id int, txt string")
    all_null = spark.createDataFrame([(1, None), (2, None)], "id int, txt string")

    assert _attach_upper(spark, empty, "txt").collect() == []
    assert sorted((r.id, r.out) for r in _attach_upper(spark, all_null, "txt").collect()) == [
        (1, []),
        (2, []),
    ]


def test_attach_extractions_accepts_dotted_column_names(spark):
    df = spark.createDataFrame([("a", "x"), (None, "y")], "`raw.txt` string, `Rev. no` string")

    result = _attach_upper(spark, df, "raw.txt", output_col="out.col")

    assert result.columns == ["raw.txt", "Rev. no", "out.col"]
    assert sorted((tuple(r) for r in result.collect()), key=lambda r: r[1]) == [
        ("a", "x", ["A"]),
        (None, "y", []),
    ]
