"""NLP pipeline for normalising entities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, Normalizer, StopWordsCleaner, Stemmer
from pyspark.ml import Pipeline

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class NLPPipeline:
    """Class for generating and applying the NLP pipeline for normalising entities."""

    google_stop_words = (
        "about above after again against all am an and any are aren't as at be because "
        "been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down "
        "during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers "
        "herself him himself his how how's i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't "
        "my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll "
        "she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they "
        "they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've "
        "were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't "
        "you you'd you'll you're you've your yours yourself yourselves"
    ).split()

    # Combine with ["a", "i"] and their capitalized versions
    all_stop_words = ["a", "i"] + google_stop_words + [word.capitalize() for word in google_stop_words]

    @classmethod
    def generate_pipeline(
        cls: type[NLPPipeline],
        input_col_name: str, 
        output_col_names: list[str]
    ) -> Pipeline:
        """Generate NLP pipeline for normalising entities.

        There are two tracks:
        1. document -> tokenTerm -> stopTerm -> cleanTerm -> term
        2. document -> tokenSymbol -> symbol
        
        Args:
            input_col_name (str): Name of the column to process as input.
            output_col_names (list[str]): Names of the columns to be processed for output.

        Returns:
            Pipeline: NLP pipeline.
        """
        document_assembler = (
            DocumentAssembler()
            .setInputCol(input_col_name)
            .setOutputCol("document")
        )

        tokenizer = (
            Tokenizer()
            .setSplitChars(["-", "/", ":", ",", ";"])
            .setInputCols(["document"])
            .setOutputCol("tokenTerm")
            .setLazyAnnotator(True)
        )

        tokenizer_symbol = (
            Tokenizer()
            .setSplitChars([":", ",", ";"])
            .setInputCols(["document"])
            .setOutputCol("tokenSymbol")
            .setLazyAnnotator(True)
        )

        normaliser_symbol = (
            Normalizer()
            .setInputCols(["tokenSymbol"])
            .setOutputCol("symbol")
            .setLowercase(True)
            .setCleanupPatterns(["[^\\w\\d\\s]", "[-]", "[/]", "[,]"])
            .setLazyAnnotator(True)
        )

        cleaner = (
            StopWordsCleaner()
            .setCaseSensitive(True)
            .setStopWords(cls.all_stop_words)
            .setInputCols(["tokenTerm"])
            .setOutputCol("stopTerm")
            .setLazyAnnotator(True)
        )

        normaliser = (
            Normalizer()
            .setInputCols(["stopTerm"])
            .setOutputCol("cleanTerm")
            .setLowercase(True)
            .setCleanupPatterns(["[^\\w\\d\\s]", "[-]", "[/]"])
            .setLazyAnnotator(True)
        )

        stemmer = (
            Stemmer()
            .setInputCols(["cleanTerm"])
            .setOutputCol("term")
            .setLazyAnnotator(True)
        )

        finisher = (
            Finisher()
            .setInputCols(output_col_names)
            .setIncludeMetadata(False)
        )

        pipeline = Pipeline(stages=[
            document_assembler,
            tokenizer,
            tokenizer_symbol,
            normaliser_symbol,
            cleaner,
            normaliser,
            stemmer,
            finisher
        ])

        return pipeline
    
    @classmethod
    def apply_pipeline(
        cls: type[NLPPipeline], 
        df: DataFrame, 
        input_col_name: str
    ) -> DataFrame:
        """Apply a generated NLP pipeline for normalising entities.

        Args:
            df (DataFrame): DataFrame of entities to be normalised.
            input_col_name (str): Name of the column to process as input.

        Returns:
            DataFrame: DataFrame of normalised entities.
        """
        pipeline = cls.generate_pipeline(input_col_name, ["term", "symbol"])

        return pipeline.fit(df).transform(df)
