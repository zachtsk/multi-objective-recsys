import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession


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

    #########################
    # ALS Params
    #########################
    als_train_sample: float
    als_user_col: str
    als_item_col: str
    als_rating_col: str
    als_embeddings: int
    als_implicit_preferences: bool

    #########################
    # W2V Params
    #########################
    w2v_train_sample: float
    w2v_embeddings: int
    w2v_min_count: int
    w2v_sentence_col: str
    w2v_window_size: int
    w2v_max_iter: int

    # Output files
    train_fp: str
    test_fp: str
    eval_fp: str
    features_fp: str
    click_embed_fp: str
    item_embed_fp: str
    user_embed_fp: str
    als_model_fp: str
    w2v_model_fp: str
    nn_model_fp: str


@dataclass
class LocalConfig:
    #########################
    # Source files
    #########################
    data_dir: str = str(Path(__file__).parent.parent / "data")
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
    als_model_fp: str = os.path.join(data_dir, "als.model")
    w2v_model_fp: str = os.path.join(data_dir, "w2v.model")
    nn_model_fp: str = os.path.join(data_dir, "neighbor.model")


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

    #########################
    # ALS Params
    #########################
    als_train_sample: float = 1.0
    als_user_col: str = "session"
    als_item_col: str = "aid"
    als_rating_col: str = "viewed"
    als_embeddings: int = 8
    als_implicit_preferences: bool = True

    #########################
    # W2V Params
    #########################
    w2v_train_sample: float = 0.1
    w2v_embeddings: int = 8
    w2v_min_count: int = 1
    w2v_sentence_col: str = "aid_ls"
    w2v_window_size: int = 5
    w2v_max_iter: int = 1

    # Output files
    train_fp: str = os.path.join(data_dir, "train.parquet")
    test_fp: str = os.path.join(data_dir, "test.parquet")
    eval_fp: str = os.path.join(data_dir, "eval.parquet")
    features_fp: str = os.path.join(data_dir, "features.parquet")
    item_embed_fp: str = os.path.join(data_dir, "item_embedding.parquet")
    user_embed_fp: str = os.path.join(data_dir, "user_embedding.parquet")
    click_embed_fp: str = os.path.join(data_dir, "click_embedding.parquet")
    als_model_fp: str = os.path.join(data_dir, "als.model")
    w2v_model_fp: str = os.path.join(data_dir, "w2v.model")
    nn_model_fp: str = os.path.join(data_dir, "neighbor.model")
