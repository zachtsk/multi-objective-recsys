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


@dataclass
class GoogleCloudConfig:
    # Source/ files
    # TODO: change the default bucket path
    data_dir: str = os.getenv("GCP_DATA_BUCKET", "gs://otto-dataproc-gpu/multi-obj-recsys")
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
