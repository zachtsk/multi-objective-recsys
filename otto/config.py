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
    # ALS
    #########################
    als_train_sample: float
    als_user_col: str
    als_item_col: str
    als_rating_col: str
    als_embeddings: int
    als_implicit_preferences: bool
    # Outputs
    item_embed_fp: str
    user_embed_fp: str
    click_embed_fp: str
    als_model_fp: str
    nn_model_fp: str

    #########################
    # W2V
    #########################
    w2v_vector_size: int
    w2v_min_count: int
    w2v_window: int
    w2v_negative: int
    w2v_ns_exponent: float
    w2v_sg: int
    # Outputs
    w2v_item_embed_fp: str
    w2v_neighbors_fp: str
    w2v_clicks_fp: str

    # Output files
    train_fp: str
    test_fp: str
    eval_fp: str
    features_fp: str


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

    #########################
    # ALS
    #########################
    als_train_sample: float = 1.0
    als_user_col: str = "session"
    als_item_col: str = "aid"
    als_rating_col: str = "viewed"
    als_embeddings: int = 8
    als_implicit_preferences: bool = True
    # Outputs
    item_embed_fp: str = os.path.join(data_dir, "item_embedding.parquet")
    user_embed_fp: str = os.path.join(data_dir, "user_embedding.parquet")
    click_embed_fp: str = os.path.join(data_dir, "click_embedding.parquet")
    als_model_fp: str = os.path.join(data_dir, "als.model")
    nn_model_fp: str = os.path.join(data_dir, "neighbor.model")

    #########################
    # W2V
    #########################
    w2v_vector_size: int = 64
    w2v_min_count: int = 1
    w2v_window: int = 3
    w2v_negative: int = 8
    w2v_ns_exponent: float = 0.2
    w2v_sg: int = 1
    # Outputs
    w2v_item_embed_fp: str = os.path.join(data_dir, "w2v_item_embedding.parquet")
    w2v_neighbors_fp: str = os.path.join(data_dir, "w2v_neighbors.parquet")
    w2v_clicks_fp: str = os.path.join(data_dir, "w2v_clicks.parquet")

    # Output files
    train_fp: str = os.path.join(data_dir, "train.parquet")
    test_fp: str = os.path.join(data_dir, "test.parquet")
    eval_fp: str = os.path.join(data_dir, "eval.parquet")
    features_fp: str = os.path.join(data_dir, "features.parquet")


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
    # ALS
    #########################
    als_train_sample: float = 1.0
    als_user_col: str = "session"
    als_item_col: str = "aid"
    als_rating_col: str = "viewed"
    als_embeddings: int = 8
    als_implicit_preferences: bool = True
    # Outputs
    item_embed_fp: str = os.path.join(data_dir, "item_embedding.parquet")
    user_embed_fp: str = os.path.join(data_dir, "user_embedding.parquet")
    click_embed_fp: str = os.path.join(data_dir, "click_embedding.parquet")
    als_model_fp: str = os.path.join(data_dir, "als.model")
    nn_model_fp: str = os.path.join(data_dir, "neighbor.model")

    #########################
    # W2V
    #########################
    w2v_vector_size: int = 64
    w2v_min_count: int = 1
    w2v_window: int = 3
    w2v_negative: int = 8
    w2v_ns_exponent: float = 0.2
    w2v_sg: int = 1
    # Outputs
    w2v_item_embed_fp: str = os.path.join(data_dir, "w2v_item_embedding.parquet")
    w2v_neighbors_fp: str = os.path.join(data_dir, "w2v_neighbors.parquet")
    w2v_clicks_fp: str = os.path.join(data_dir, "w2v_clicks.parquet")

    # Output files
    train_fp: str = os.path.join(data_dir, "train.parquet")
    test_fp: str = os.path.join(data_dir, "test.parquet")
    eval_fp: str = os.path.join(data_dir, "eval.parquet")
    features_fp: str = os.path.join(data_dir, "features.parquet")
