import logging
from pathlib import Path
from typing import Tuple

import typer
import pandas as pd
from typing_extensions import Annotated

from adaptation_reviewer.download import download_paper
from adaptation_reviewer.pbar import progress_bar
from adaptation_reviewer.process import (
    create_connection,
    create_embeddings_abstracts,
    create_table,
)
from adaptation_reviewer.transform import generate_parquet_from_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


app = typer.Typer(help="Tool to review adaptation papers")

VERBOSE_HELP = "Verbose mode"


@app.command()
def transform_crossref(
    path_to_cf: str = typer.Argument(..., help="Path to raw crossref data"),
    path_to_parquet: str = typer.Argument(
        ..., help="Path to saved parquet files"
    ),
    row_group_size: int = typer.Option(
        100_000, help="Row group size for parquet files"
    ),
    rows_per_file: int = typer.Option(
        1_000_000, help="Number of rows per parquet file"
    ),
    verbose: bool = typer.Option(False, help=VERBOSE_HELP),
    flatten_authors: bool = typer.Option(False, help="Flatten the author data"),
    n_jobs: int = typer.Option(5, help="Number of jobs to use. Use -1 for all"),
) -> None:
    """
    Transform a list of DOIs records from JSON into a parquet files
    """

    cf_paths = Path(path_to_cf).rglob("*.json.gz")

    if verbose:
        logger.setLevel(logging.INFO)
        logger.info("Verbose mode")

    generate_parquet_from_list(
        cf_paths,
        path_to_parquet,
        row_group_size,
        rows_per_file,
        n_jobs,
        flatten_authors,
    )

    return None


@app.command()
def create_db(
    db_name: str = typer.Argument(..., help="Name of the database"),
    path_parquet: str = typer.Argument(..., help="Path to the parquet file"),
    keywords: Annotated[
        Tuple[str, str, str],
        typer.Option(help="Keywords to filter the abstracts"),
    ] = ("adaptation", "mortality", "temperature"),
    table_name: str = typer.Option("papers", help="Name of the table"),
) -> None:
    """
    Create a DuckDB database with DOI records from Parquet files
    """
    con = create_connection(db_name=db_name)

    create_table(
        path_parquet=path_parquet,
        keyords=list(keywords),
        table_name=table_name,
        con=con,
    )

    return None


@app.command()
def embed_abstracts(
    db_name: str = typer.Argument(
        ..., help="Name of the database or file to embed"
    ),
    table_name: str = typer.Option("papers", help="Table in DB to embed"),
    model: str = typer.Option("all-mpnet-base-v1", help="Name of the model"),
    key: str = typer.Option(
        "uuid", help="Column to use as paper unique identifier"
    ),
    abstract_key: str = typer.Option("abstract", help="Column with abstracts"),
    csv_file: str = typer.Option(
        None, help="CSV file to embed. Optional if data not in DB"
    ),
) -> None:
    """
    Use a LLM to embed the abstracts of DOI records the database
    """
    con = create_connection(db_name=db_name)

    if csv_file is not None:
        df = pd.read_csv(csv_file)

        # Parse column names to be lowercase and replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        key = key.lower()
        abstract_key = abstract_key.lower()

        df.rename(columns={key: "uuid", abstract_key: "abstract"}, inplace=True)

        con.sql(
            f"DROP TABLE {table_name}; CREATE TABLE {table_name} AS (SELECT * FROM df)",
        )

    create_embeddings_abstracts(
        model=model,
        table_name=table_name,
        key="uuid",
        abstract_key="abstract",
        con=con,
    )

    return None


@app.command()
def download(
    db_name: str = typer.Argument(..., help="Name of the database"),
    table_name: str = typer.Option(
        "papers", help="Table which DOIs we want to download papers "
    ),
    path_pdfs: str = typer.Option(
        "papers", help="Path to save the downloaded papers"
    ),
) -> None:
    """
    Download papers from the DOIs in the database
    """
    con = create_connection(db_name=db_name)

    papers = con.execute(f"SELECT * FROM {table_name}").df()

    with progress_bar as p:
        for i in p.track(range(len(papers) - 1)):
            doi_url = papers.loc[i, "doi"]
            uuid = papers.loc[i, "uuid"]

            download_paper(
                doi_url, uuid, path_pdfs, password="torpasslitreview"
            )

    return None


if __name__ == "__main__":
    app()
