import logging
from typing import List
import numpy as np
import pandas as pd
import duckdb
from sentence_transformers import SentenceTransformer

from adaptation_reviewer.pbar import progress_bar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_connection(db_name: str = None) -> duckdb.duckdb.DuckDBPyConnection:
    if not db_name:
        db_name = ":memory:"

    con = duckdb.connect(database=db_name, read_only=False)

    # Use vector similarity extension
    con.execute("INSTALL vss;")
    con.execute("LOAD vss;")

    logger.debug("Connection to DuckDB created in {db_name}")

    return con


def create_table(
    path_parquet: str,
    keyords: str | List,
    table_name: str,
    con=None,
    create_table: bool = True,
    **kwargs,
) -> None | pd.DataFrame:
    """Create a SQL filtered table by keywords in the abstract

    This query creates a table with a sepcific name and using keywords in the
    abstract. The keywords will be used as where arguments.

    Parameters
    ----------
    path_parquet : str
        Path to the parquet file
    keyords : str or list
        Keywords to filter the abstracts
    table_name : str
        Name of the table to create
    con : duckdb.duckdb.DuckDBPyConnection, optional
        Connection to the database. The default is None.
    kwargs : dict
        Keyword arguments to pass to the `create_connection` function
    """

    if not con:
        con = create_connection(**kwargs)

    if isinstance(keyords, list):
        keyords = " AND ".join(
            [f"regexp_matches(abstract, '{key}', 'i')" for key in keyords]
        )
    else:
        keyords = f"regexp_matches(abstract, '{keyords}', 'i')"

    query = f"""
        SELECT 
        LEFT(sha256(doi), 10) AS uuid,
        doi,
        title,
        family_1 || ', ' || given_1 AS first_author, 
        year::INT AS year,
        TRIM(
            regexp_replace(
                regexp_replace(
                    regexp_replace(abstract, 'Abstract', '', 'g'),
                    '<[^>]+>', '', 'g'
                ),
                '\n', ' ', 'g'
            )
            ) AS abstract,
        FROM read_parquet('{path_parquet}/*.parquet', union_by_name=True)
        WHERE {keyords}
        AND type = 'journal-article'
        AND abstract IS NOT NULL
        AND family_1 IS NOT NULL
        """

    if create_table:
        # Create table
        con.execute(
            f"""
            DROP TABLE IF EXISTS {table_name};
            """
        )

        query = f"CREATE TABLE {table_name} AS ({query});"

        con.execute(query)
        logger.debug(f"SQL Table created: {table_name}")
    else:
        query = con.sql(query)
        return query.to_df()

    return None


def create_embeddings_abstracts(
    model: str,
    table_name: str,
    con: None | duckdb.duckdb.DuckDBPyConnection = None,
    key: str = "uuid",
    abstract_key: str = "abstract",
    **kwargs,
) -> None:
    """Create embedding table in DuckDB dataset

    This function takes a DuckDB table with the `"abstract"` columns and create
    embeeings using the SentenceTransformer model. The embeddings are stored in
    a new table called `table_name_embed` with the original id.

    Parameters
    ----------
    model : str
        SentenceTransformer model to use
    table_name : str
        Name of the table to create
    con : duckdb.duckdb.DuckDBPyConnection, optional
        Connection to the database. The default is None.
    kwargs : dict
        Keyword arguments to pass to the `create_connection` function
    """

    embedding_model = SentenceTransformer(model)
    embed_size = embedding_model.get_sentence_embedding_dimension()

    abstracts = con.sql(
        f"""
        SELECT {key}, {abstract_key} FROM {table_name}
        where {abstract_key} IS NOT NULL
        """
    ).to_df()

    embeddings = embedding_model.encode(
        abstracts.abstract.tolist(), show_progress_bar=True
    )

    # Check that array is np.float32, otherwise transform it
    if embeddings.dtype != np.float32:
        embeddings = embeddings.astype("float32")

    if not con:
        con = create_connection(**kwargs)

    con.execute(
        f"CREATE OR REPLACE TABLE {table_name}_embed_abstracts (id STRING, embeddings FLOAT4[{embed_size}]);"
    )

    # Create a new table with the embeddings
    with progress_bar as p:
        for i in p.track(range(abstracts.shape[0] - 1), description=table_name):
            con.execute(
                f"INSERT INTO {table_name}_embed_abstracts VALUES (?, ?)",
                (abstracts.uuid.tolist()[i], embeddings[i, :]),
            )
