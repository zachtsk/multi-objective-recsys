import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    #########################
    # Source files
    #########################
    data_dir: str
    train_jsonl_fp: str
    test_jsonl_fp: str

    #########################
    # Normalized Files
    #########################
    # Sample Size
    size_sm: int
    size_med: int
    size_lg: int

    # Output files
    train_fp: str
    test_fp: str
    eval_fp: str
    features_fp: str
    click_embed_fp: str
    item_embed_fp: str
    user_embed_fp: str
    model_fp: str
    nn_model_fp: str


@dataclass
class LocalConfig:
    #########################
    # Source files
    #########################
    data_dir: str = Path(__file__).parent.parent / "data"
    train_jsonl_fp: str = str(data_dir / "train.jsonl")
    test_jsonl_fp: str = str(data_dir / "test.jsonl")

    #########################
    # Normalized Files
    #########################
    # Sample Size
    size_sm: int = 1_000
    size_med: int = 10_000
    size_lg: int = 100_000

    # Output files
    train_fp: str = str(data_dir / "train.parquet")
    test_fp: str = str(data_dir / "test.parquet")
    eval_fp: str = str(data_dir / "eval.parquet")
    features_fp: str = str(data_dir / "features.parquet")
    click_embed_fp: str = str(data_dir / "click_embedding.parquet")
    item_embed_fp: str = str(data_dir / "item_embedding.parquet")
    user_embed_fp: str = str(data_dir / "user_embedding.parquet")
    model_fp: str = str(data_dir / "als.model")
    nn_model_fp: str = str(data_dir / "neighbor.model")


@dataclass
class GoogleCloudConfig:
    # Source/ files
    # TODO: change the default bucket path
    data_dir: str = os.getenv("GCP_DATA_BUCKET", "gs://otto-dataproc-gpu/otto/data")
    train_jsonl_fp: str = os.path.join(data_dir, "train.jsonl")
    test_jsonl_fp: str = os.path.join(data_dir, "test.jsonl")

    #########################
    # Normalized Files
    #########################
    # Sample Size
    size_sm: int = 1_000
    size_med: int = 10_000
    size_lg: int = 100_000

    # Output files
    train_fp: str = os.path.join(data_dir, "train.parquet")
    test_fp: str = os.path.join(data_dir, "test.parquet")
    eval_fp: str = os.path.join(data_dir, "eval.parquet")
    features_fp: str = os.path.join(data_dir, "features.parquet")
    item_embed_fp: str = os.path.join(data_dir, "item_embedding.parquet")
    user_embed_fp: str = os.path.join(data_dir, "user_embedding.parquet")
    click_embed_fp: str = os.path.join(data_dir, "click_embedding.parquet")
    model_fp: str = os.path.join(data_dir, "als.model")
    nn_model_fp: str = os.path.join(data_dir, "neighbor.model")
