"""Internal extraction logic for NER pipelines.

This module contains the core logic for running NER inference on text data.
It's separated from _pipelines.py which handles model loading/creation.
"""

from __future__ import annotations

import re
import torch


def extract_with_regex(text: str, patterns: list[str]) -> set:
    """Extract biologics and common drug patterns using regex.
    
    Args:
        text: Input text to extract from
        patterns: List of regex patterns to match
        
    Returns:
        Set of extracted drug names (lowercase)
    """
    drugs = set()
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            match_clean = match.strip().lower()
            # Filter: length > 3, doesn't start with digit
            if len(match_clean) > 3 and not match_clean[0].isdigit():
                drugs.add(match_clean)
    return drugs


def extract_entities_from_batch(
    texts: list[str], 
    ner_pipeline,
    label_filters: list[str]
) -> list[set]:
    """Extract drug entities from a batch of texts using NER.
    
    Args:
        texts: List of text strings to process
        ner_pipeline: Transformers NER pipeline
        label_filters: Entity labels to keep (e.g., ['CHEMICAL', 'DRUG'])
        
    Returns:
        List of sets, one per input text, containing extracted drug names
    """
    if not texts:
        return []
    
    try:
        with torch.no_grad():
            batch_entities = ner_pipeline(texts)
        
        # Handle both single result and list of results
        if not isinstance(batch_entities[0], list):
            batch_entities = [batch_entities]
        
        results = []
        for entities in batch_entities:
            drugs = set()
            for ent in entities:
                label = ent.get('entity_group', '').upper()
                if any(filter_label in label for filter_label in label_filters):
                    # Clean subword tokens (##)
                    word = ent['word'].strip().replace(' ##', '').replace('##', '').strip()
                    # Filter out single characters and pure numbers
                    if len(word) > 1 and not word.isdigit():
                        drugs.add(word.lower())
            results.append(drugs)
        
        return results
    
    except Exception as e:
        print(f"NER batch failed: {e}")
        return [set() for _ in texts]
