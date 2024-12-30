"""Transform all JSON files to Parquet

This script just reads JSON files in any format and pass them to Parquet format
This is just an intermediate step to pass the data to DuckDB in the most eicient
way possible. This code assumes there's pandas and Dask installed in the
environment.
"""

import gzip
import json
import logging
import os
from itertools import batched
from pathlib import Path
from time import time
from typing import List, Tuple

import pandas as pd
from joblib import Parallel, delayed
from joblib_progress import joblib_progress

from adaptation_reviewer.utils import (
    list_to_datetime,
    standardize_headers,
    flatten_author,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def open_compressed_json(file_path: Path) -> List[dict]:
    """Open a compressed JSON file

    This function reads a compressed JSON file and returns the data as a list of
    dictionaries. This function is meant to be used in the `generate_parquet`
    function.

    Parameters
    ----------
    file_path : Path
        Path to the compressed JSON file

    Returns
    -------
    list
        List of dictionaries with the JSON data
    """

    subset_cols = [
        "DOI",
        "title",
        "URL",
        "published",
        "created",
        "publisher",
        "is-referenced-by-count",
        "container-title",
        "reference-count",
        "type",
        "volume",
        "journal-issue",
        "language",
        "author",
        "link",
        "abstract",
    ]

    with gzip.open(file_path, "rb") as f:
        records = json.load(f)["items"]

    # Filter the dictionaries to keep only the keys in subset_cols
    records = [
        {key: record.get(key, None) for key in subset_cols}
        for record in records
    ]

    return records


def json_records_to_parquet(
    list_files: Tuple[Path],
    output_dir: str,
    row_group_size: int = 100_000,
    flatten_authors: bool = False,
) -> None:
    """Transform list of JSON files to Parquet"""

    list_files = list(list_files)

    list_files.sort(key=lambda x: int(x.stem.split(".")[0]))
    min_file, max_file = (
        int(list_files[0].stem.split(".")[0]),
        int(list_files[-1].stem.split(".")[0]),
    )

    file_name = f"{min_file}_{max_file}.parquet"

    df = [pd.json_normalize(open_compressed_json(file)) for file in list_files]
    df = pd.concat(df, ignore_index=True)

    # Standardize headers
    df = standardize_headers(df)

    # Do some cleaning
    df["year"] = df["published_date_parts"].apply(list_to_datetime)
    # Parse stuff as strings, not lists
    df["container_title"] = df.container_title.str[0]
    df["title"] = df.title.str[0]

    # Flatten authors
    if flatten_authors:
        # Remove all stuff without author elements
        df = df[~df.author.isna()]

        # Do our own et. al. here
        df.loc[:, "author"] = df.author.apply(lambda x: x[:4])

        df = flatten_author(df)

    df.to_parquet(
        os.path.join(output_dir, file_name),
        index=False,
        row_group_size=row_group_size,
    )

    logger.debug(f"Saving {file_name} to {output_dir}")

    return None


def generate_parquet_from_list(
    compressed_json_files: List[Path],
    output_dir: str | Path,
    row_group_size: int = 100_000,
    rows_per_file: int = 1_000_000,
    n_jobs: int = -1,
    flatten_authors: bool = False,
    json_file_record_default: int = 5_000,
) -> None:
    """Transform list of JSON files to Parquet

    This function takes a list of `.json.gz` files and extract the records as
    a set of Parquet files. The function will make sure that parquet files are
    at least as big as `rows_per_file`, so will concatenate several files up to
    this limit. The function will also use the `row_group_size` parameter to
    save the parquet files in a way that is easier to read by DuckDB.

    Parameters
    ----------
    compressed_json_files : Path or List[Path]
        Paths to the compressed JSON file
    rows_per_file : int
        Number of rows per Parquet file
    row_group_size : int
        Number of rows per row group in the Parquet file
    """

    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    if not output_dir.exists():
        output_dir.mkdir()

    # Keep stuff sorted and organized
    compressed_json_files = list(compressed_json_files)
    compressed_json_files.sort(key=lambda x: int(x.stem.split(".")[0]))

    chunk_size = rows_per_file // json_file_record_default

    logger.info(f"Chuking JSON files into {chunk_size:,} .parquet files")
    start_time = time()

    # Build chunks
    file_chunks = batched(compressed_json_files, chunk_size)

    # Process in parallel
    with joblib_progress(
        "JSON --> Parquet in parallel",
        total=len(compressed_json_files) // chunk_size,
    ):
        Parallel(n_jobs=n_jobs)(
            delayed(json_records_to_parquet)(
                chunk, output_dir, row_group_size, flatten_authors
            )
            for chunk in file_chunks
        )

    end_time = time()
    logger.info(
        f"Extracted {len(compressed_json_files):,} files in {end_time - start_time:.2f} seconds"
    )
    return None
