import json
import pandas as pd
import pytest
from pathlib import Path


def load_data(file_path: str):
    """Load data from a JSON or Parquet file."""
    if file_path.endswith(".json"):
        with open(file_path, "r") as f:
            return json.load(f)
    elif file_path.endswith(".parquet"):
        return pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format")


@pytest.fixture
def sample_json():
    return load_data("tests/data/sample.json")


@pytest.fixture
def sample_parquet():
    return load_data("tests/data/sample.parquet")
