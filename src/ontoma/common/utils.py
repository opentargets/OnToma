"""Utility functions."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def annotate_entity(c: Column, entity_score: float, nlp_pipeline_track: str, entity_source: str) -> Column:
    """Annotate entity with score and the NLP pipeline to be processed with.
    
    Args:
        c (Column): Column containing entity label.
        entity_score (float): Score of the entity.
        nlp_pipeline_track (str): NLP pipeline track to be used.
        entity_source (str): Source of the entity.
    
    Returns:
        Column: Column of struct of annotated entities.
    """
    return f.transform(
        # Replace null with empty array
        f.coalesce(c, f.array()),
        lambda x: f.struct(
            x.alias("entityLabel"),
            f.lit(entity_score).alias("entityScore"),
            f.lit(nlp_pipeline_track).alias("nlpPipelineTrack"),
            f.lit(entity_source).alias("entitySource")
        )
    )

def translate_greek_alphabet(label: Column) -> Column:
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

def translate_special_characters(label: Column) -> Column:
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
            translate_special_characters(translate_greek_alphabet(label)),
            translate_greek_alphabet(label)
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
