"""Internal model loading and creation logic for NER pipelines.

This module handles one-time model setup: loading from HuggingFace,
device detection, and pipeline configuration. For running inference on data, see _extractors.py.
"""

from __future__ import annotations

from transformers import (
            pipeline,
            AutoTokenizer,
            AutoModelForTokenClassification,
        )
import torch


def get_device():
    """Detect and return the best available device for inference.
    
    Returns:
        Device identifier: "mps" for Apple Silicon, 0 for CUDA GPU, -1 for CPU
    """
    if torch.backends.mps.is_available():
        return "mps"
    elif torch.cuda.is_available():
        return 0
    else:
        return -1


def create_ner_pipeline(
    model_name: str,
    tokenizer_name: str | None = None,
    aggregation_strategy: str = "max"
):
    """Create a generic NER pipeline from HuggingFace models.
    
    Args:
        model_name: HuggingFace model identifier for token classification
        tokenizer_name: HuggingFace tokenizer identifier (defaults to model_name)
        aggregation_strategy: Strategy for aggregating subword tokens (default: "max")
        
    Returns:
        Transformers NER pipeline configured for the specified model
    """
    device = get_device()
    
    # Use model name for tokenizer if not specified
    if tokenizer_name is None:
        tokenizer_name = model_name
    
    # Load model and tokenizer
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
    model = AutoModelForTokenClassification.from_pretrained(model_name)
    
    # Move model to appropriate device
    if device == "mps":  # Apple Silicon
        model = model.to("mps")
    elif device == 0:  # CUDA
        model = model.to("cuda")
    # CPU: no explicit move needed
    
    # Create pipeline
    ner = pipeline(
        "ner",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy=aggregation_strategy,
        device=device,
        ignore_labels=[],
    )
    
    return ner


