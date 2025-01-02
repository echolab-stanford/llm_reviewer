import os
import json
import pytest
from typer.testing import CliRunner
from adaptation_reviewer.cli import app
from tests.conftest import load_data

runner = CliRunner()


@pytest.fixture
def sample_json():
    return load_data("tests/data/sample.json")


def test_transform_crossref(sample_json, tmp_path):
    sample_file = tmp_path / "sample.json"
    sample_file.write_text(json.dumps(sample_json))
    result = runner.invoke(
        app,
        [
            "transform-crossref",
            "--path_json_files",
            str(sample_file),
            "--save_path",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0
    assert os.path.exists(tmp_path / "sample.parquet")
