import pandas as pd
import numpy as np
from typing import List


def list_to_datetime(date_list):
    """Aux function to convert a list to get the year only"""

    if isinstance(date_list, list):
        return date_list[0][0]
    elif isinstance(date_list, float):
        if pd.isna(date_list):
            return np.nan
        else:
            return int(date_list)
    else:
        raise ValueError("Invalid date list format")


def standardize_headers(df: pd.DataFrame, func=None) -> pd.DataFrame:
    """Helper function to standarize column names for an arbitrary DataFrame

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be standarized
    func : callable, optional
        A function to apply to the DataFrame. The default is None.

    Returns
    -------
    pd.DataFrame
    """

    df.columns = df.columns.str.replace("[.-]", "_", regex=True).str.lower()
    if func:
        df = df.apply(func)

    return df


def flatten_author(df: pd.DataFrame) -> pd.DataFrame:
    """Extract flattened author data from a list of nested dictionaries

    This function takes the raw Crossref data and extracts the author data that
    is stored as a nested dictionary. This function extracts the author data and
    creates columns with the author dictionary elements: given, family, and
    affiliation. If the paper has more than one author, then the function will
    return `given_1`, `given_2`, `family_1`, `family_2`, etc. If the paper has
    more than 4 papers, then the function will only return these.

    Notice this function applied the `affiliation_normalization` function.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with the raw Crossref data

    Note:
    ----
     - Ideally we will add a `et_al` column, so far we just ignore it.
     - Some of the Crossref papers have up to 60 authors, thus we decide to
     cut that.

    Returns
    -------
    pd.DataFrame

    """
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


def normalize_affiliation(affiliation: list | pd.Series) -> List:
    """Extract author's affiliation from Crossref JSON raw data

    The affiliation field in the Crossref JSON raw data is a list of dictionaries
    that is hard to parse in DuckDB. This function extracts all of author's
    affiliation and returns a list with all the names with the same order. This
    function is meant to be map to a DataFrame column.

    Parameters
    ----------
    affiliation : list or pd.Series
        List of dictionaries with affiliation data

    Returns
    -------
        List with flattened affiliations
    """
    if isinstance(affiliation, float):
        return None

    if len(affiliation) == 0:
        return None

    normalized = []
    for item in affiliation:
        if isinstance(item, dict) and "name" in item:
            normalized.append(item["name"])
    return normalized
