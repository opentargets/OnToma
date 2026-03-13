"""Utility functions."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def annotate_entity(c: Column, nlp_pipeline_track: str, entity_score: float, entity_source: str) -> Column:
    """Annotate entity with score and the NLP pipeline to be processed with.
    
    Args:
        c (Column): Column containing entity label.
        nlp_pipeline_track (str): NLP pipeline track to be used.
        entity_score (float): Score of the entity.
        entity_source (str): Source of the entity.
    
    Returns:
        Column: Column of struct of annotated entities.
    """
    return f.transform(
        # Replace null with empty array
        f.coalesce(c, f.array()),
        lambda x: f.struct(
            x.alias("entityLabel"),
            (
                determine_track(x) if nlp_pipeline_track == "tbd"
                else f.lit(nlp_pipeline_track)
            ).alias("nlpPipelineTrack"),
            f.lit(entity_score).alias("entityScore"),
            f.lit(entity_source).alias("entitySource")
        )
    )

def determine_track(label: Column) -> Column:
    """Return the NLP pipeline track to be used, based on particular criteria.

    There are two potential tracks: "term" and "symbol".

    The label will be processed using the "symbol" track if it is an acronym,
    determined using the following criteria:
    - does not contain spaces, and
    1. is 6 characters or less, or
    2. is 11 characters or less, and contains 50% or more uppercase letters

    Args:
        label (Column): Column containing entity label.

    Returns:
        Column: Column specifying the NLP pipeline track to be used.
    """
    return f.when(
        (
            ~label.contains(" ") & 
            (
                (f.length(label) <= 6) | 
                ((f.length(label) <= 11) & (_uppercase_proportion(label) > 0.5))
            )
        ),
        f.lit("symbol")
    ).otherwise(f.lit("term"))

def _uppercase_proportion(label: Column) -> Column:
    """Return the proportion of uppercase letters in the label.

    The number of uppercase letters is divided by the number of alphabetic characters.
    Digits and symbols are disregarded for this calculation.

    Args:
        label (Column): Column containing entity label.

    Returns:
        Column: Column specifying the proportion of uppercase letters in the label.
    """
    num_uppercase = f.length(f.regexp_replace(label, "[^A-Z]", ""))
    num_letters = f.length(f.regexp_replace(label, "[^A-Za-z]", ""))

    return f.when(num_letters == 0, None).otherwise(num_uppercase / num_letters)

def _translate_greek_alphabet(label: Column) -> Column:
    """Translate greek alphabet into latin alphabet.

    Translations are based on https://www.rapidtables.com/math/symbols/greek_alphabet.html

    Args:
        label (Column): Column containing the label to be translated.

    Returns:
        Column: Column containing the translated label.
    """
    return (
        f.translate(
            label,
            "αβγδεζηικλμνξπρτυω",
            "abgdezhiklmnxprtuo"
        )
    )

def _translate_special_characters(label: Column) -> Column:
    """Translate accented latin characters into latin alphabet.

    Translations are based on https://en.wikipedia.org/wiki/Latin-1_Supplement

    Args:
        label (Column): Column containing the label to be translated.

    Returns:
        Column: Column containing the translated label.
    """
    return (
        f.translate(
            label,
            "àèìòùáéíóúâêîôûäëïöüÀÈÌÒÙÁÉÍÓÚÂÊÎÔÛÄËÏÖÜãåõøÃÅÕØçñýÇÑÝ",
            "aeiouaeiouaeiouaeiouAEIOUAEIOUAEIOUAEIOUaaooAAOOcnyCNY"
        )
    )

def get_alternative_translations(label: Column) -> Column:
    """Provides an array of labels translated in two different ways.

    Non-latin alphabet characters in labels should be translated, but 
    special characters should not always be translated 
    as some labels contain special characters due to character encoding mismatches.
    Labels are processed both ways to account for this.

    Example label that should be translated: 
    Papillon-Lefèvre syndrome

    Example label that should be not translated (special characters will be dropped in downstream steps): 
    small, protruding, ‚Äúcup-shaped‚Äù ears

    Args:
        label (Column): Column containing the label to be processed.

    Returns:
        Column: Column containing array of alternative translations.
    """
    return (
        f.array(
            _translate_special_characters(_translate_greek_alphabet(label)),
            _translate_greek_alphabet(label)
        )
    )

def clean_disease_label(disease_label: Column) -> Column:
    """Clean disease label by removing prefixes.

    Args:
        disease_label (Column): Column containing the disease label with prefixes.

    Returns:
        Column: Column containing the disease label with prefixes removed.
    """
    return (
        f.when(
            disease_label.contains("#"),
            f.regexp_extract(
                f.element_at(f.split(disease_label, "#"), -1), 
                r'^(?:[A-Z]{1}[0-9]{2}[-.A-Z0-9]* |Chapter [IVX]+ )?(.+)$', 
                1
            )
        ).otherwise(disease_label)
    )

def filter_disease_crossrefs(disease_df: DataFrame) -> DataFrame:
    """Filter out disease crossrefs with irrelevant prefixes.

    Args:
        disease_df (DataFrame): DataFrame containing disease crossrefs.

    Returns:
        DataFrame: DataFrame containing only the relevant disease crossrefs.
    """
    prefix_list = ["PMID", "DOI:", "ORCID", "PERSON", "ISBN", "WIKIPEDIA", "HTTP", "QUANT", "UM-BBD_PATHWAYID"]

    filter_condition = reduce(
        lambda cond1, cond2: cond1 | cond2,
        [f.col("entityLabel").contains(prefix) for prefix in prefix_list],
        f.lit(False)
    )
    
    return disease_df.filter(~filter_condition)

def format_identifier(identifier: Column) -> Column:
    """Format identifier to have consistent formatting.

    Args:
        identifier (Column): Column containing the identifier.

    Returns:
        Column: Column containing the formatted identifier.
    """
    # ensure consistent formatting across identifiers
    identifier = (
        f.when(
            f.length(f.regexp_extract(identifier, r'^.+:(.+_.+)$', 1)) > 1,
            f.regexp_extract(identifier, r'^.+:(.+_.+)$', 1)
        ).otherwise(identifier)
    )
    identifier = f.regexp_replace(identifier, "_", ":")
    
    # ensure Orphanet identifiers are consistent
    return f.regexp_replace(identifier, r'ORDO:|ORPHA:', "ORPHANET:")
