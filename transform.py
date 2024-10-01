"""Transform all JSON files to Parquet

This script just reads JSON files in any format and pass them to Parquet format
This is just an intermediate step to pass the data to DuckDB in the most eicient
way possible. This code assumes there's pandas and Dask installed in the
environment.
"""

import os
import argparse
import dask
import pandas as pd
from pathlib import Path


def standardize_headers(df: pd.DataFrame, func=None) -> pd.DataFrame:
    df.columns = df.columns.str.replace("-", "_").str.lower()
    if func:
        df = df.apply(func)

    return df


def normalize_affiliation(affiliation):
    if len(affiliation) == 0:
        return None

    normalized = []
    for item in affiliation:
        if isinstance(item, dict) and "name" in item:
            normalized.append(item["name"])
    return normalized


def flatten_author(df: pd.DataFrame) -> pd.DataFrame:
    # Explore author columns to max 4 (et.al. limit)
    df_exp = df.explode("author")

    df_auth = df_exp.author.apply(lambda x: pd.Series(x))
    df_auth["doi"] = df_exp.doi.values
    df_auth["auth_num"] = df_auth.groupby("doi").cumcount() + 1

    # Make it wide
    df_auth = df_auth.pivot(
        index="doi",
        columns="auth_num",
        values=["given", "family", "affiliation"],
    ).reset_index()

    # Fancy rename to get author_1, author_2, etc
    df_auth.columns = [f"{a}_{b}" for a, b in df_auth.columns]

    # Normalize affiliation to only get names in a list. Make life easier to DuckDB
    affiliation_cols = df_auth.filter(like="affiliation_")
    df_auth[affiliation_cols.columns] = affiliation_cols.applymap(
        normalize_affiliation
    )

    # Merge stuff
    df = df_auth.merge(df, left_on="doi_", right_on="doi")
    df.drop(columns="doi_", inplace=True)

    return df


def json_to_parquet(file_path: str, output_dir: str) -> None:
    subset_cols = [
        "DOI",
        "title",
        "URL",
        "published",
        "is-referenced-by-count",
        "container-title",
        "reference-count",
        "type",
        "volume",
        "language",
        "author",
        "abstract",
    ]

    # Make paths nicer
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Process only if the file doesn't exists
    save_path = os.path.join(
        output_dir, f"{file_path.stem.split('.')[0]}.parquet"
    )
    if not os.path.exists(save_path):
        try:
            df = pd.read_json(file_path)

            # Subset and sanitize
            df = standardize_headers(df[df.columns.intersection(subset_cols)])

            # Remove all stuff without author elements
            df = df[~df.author.isna()]

            # Do our own et. al. here
            df.loc[:, "author"] = df.author.apply(lambda x: x[:4])

            # Parse stuff as strings, not lists
            df["container_title"] = df.container_title.str[0]
            df["title"] = df.title.str[0]

            # Extract all author data
            df = flatten_author(df)

            # Define the output path
            df.to_parquet(save_path, index=False)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    return None


def process_files_in_parallel(json_files: list, output_dir: str) -> None:
    # Create saving dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok="ignore")

    tasks = [
        dask.delayed(json_to_parquet)(file, output_dir) for file in json_files
    ]

    dask.compute(*tasks)

    client.close()

    return None


if __name__ == "__main__":
    # Define input arguments
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--path_json_files",
        required=True,
        type=str,
        help="Path to JSON files. They can be compressed",
    )
    parser.add_argument(
        "--save_path",
        type=str,
        default="temp",
        help="Path to save grid.",
    )

    # Run all in Dask client!
    from dask.distributed import Client

    client = Client(n_workers=20)

    # Parse arguments
    args = parser.parse_args()

    json_files = list(Path(args.path_json_files).glob("*.json.gz"))

    process_files_in_parallel(json_files=json_files, output_dir=args.save_path)
