import os
import pytest
from typer.testing import CliRunner
from adaptation_reviewer.cli import app
from tests.conftest import load_data

runner = CliRunner()


@pytest.fixture
def sample_parquet():
    return load_data("tests/data/sample.parquet")


def test_create_db(sample_parquet, tmp_path):
    sample_file = tmp_path / "sample.parquet"
    sample_parquet.to_parquet(sample_file)
    db_path = tmp_path / "test.db"
    result = runner.invoke(
        app,
        [
            "create-db",
            "--parquet_files",
            str(sample_file),
            "--db_path",
            str(db_path),
        ],
    )
    assert result.exit_code == 0
    assert os.path.exists(db_path)
