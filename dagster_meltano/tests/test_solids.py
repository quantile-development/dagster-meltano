import json
import os
import re
from pathlib import Path

import pytest
from dagster import AssetMaterialization, build_solid_context, execute_solid

from dagster_meltano.solids import MeltanoEltSolid
from dagster_meltano.tests.test_meltano_select import _create_meltano_select
from dagster_meltano.tests.utils import len_output_file, read_output_file, reset_output_file

os.environ["MELTANO_PROJECT_ROOT"] = (Path(__file__).parents[2] / "meltano").__str__()


def test_meltano_elt_solid_explicit_params():
    result = execute_solid(
        MeltanoEltSolid("csv_to_jsonl").solid,
        input_values={
            "elt_args": {
                "tap": "tap-csv",
                "target": "target-jsonl",
                "job_id": "tap-csv-target-jsonl",
            },
        },
    )

    materializations = result.materializations_during_compute
    meltano_materialization = materializations[0]

    metadata = {}
    for entry in materializations[0].metadata_entries:
        metadata[entry.label] = entry.entry_data

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(meltano_materialization, AssetMaterialization)
    assert meltano_materialization.asset_key[0][1] == "csv_to_jsonl"
    assert metadata["tap"].text == "tap-csv"
    assert metadata["target"].text == "target-jsonl"
    assert metadata["job-id"].text == "tap-csv-target-jsonl"
    assert metadata["full-refresh"].value == 0


def test_meltano_elt_solid_nonexplicit_params():
    """Auto-generated inputs values match requirements."""
    result = execute_solid(
        MeltanoEltSolid("csv_to_jsonl").solid,
        input_values={
            "elt_args": {
                "tap": "tap-csv",
                "target": "target-jsonl",
            },
        },
    )

    materializations = result.materializations_during_compute
    meltano_materialization = materializations[0]

    metadata = {}
    for entry in materializations[0].metadata_entries:
        metadata[entry.label] = entry.entry_data

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(meltano_materialization, AssetMaterialization)
    assert meltano_materialization.asset_key[0][1] == "csv_to_jsonl"
    assert metadata["tap"].text == "tap-csv"
    assert metadata["target"].text == "target-jsonl"
    assert metadata["job-id"].text == "tap-csv-target-jsonl"
    assert metadata["full-refresh"].value == 0


def test_meltano_elt_solid_env_vars():
    """Config env vars should override input provided env vars."""
    output_filepath = Path(os.environ["MELTANO_PROJECT_ROOT"]) / "load" / "sample.jsonl"

    reset_output_file(output_filepath)
    initial_output_file_len = len_output_file(output_filepath)

    result = execute_solid(
        MeltanoEltSolid("csv_to_jsonl").solid,
        run_config={"solids": {"csv_to_jsonl": {"config": {"full_refresh": True}}}},
        input_values={
            "elt_args": {"tap": "tap-csv", "target": "target-jsonl"},
            "env_vars": {"TAP_CSV__SELECT": json.dumps(["test_value"])},
        },
    )

    final_output_file_len = len_output_file(output_filepath)
    reset_output_file(output_filepath)

    materializations = result.materializations_during_compute
    meltano_materialization = materializations[0]

    metadata = {}
    for entry in materializations[0].metadata_entries:
        metadata[entry.label] = entry.entry_data

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(meltano_materialization, AssetMaterialization)
    assert meltano_materialization.asset_key[0][1] == "csv_to_jsonl"
    assert metadata["tap"].text == "tap-csv"
    assert metadata["target"].text == "target-jsonl"
    assert metadata["job-id"].text == "tap-csv-target-jsonl"
    assert metadata["full-refresh"].value == 1
    assert initial_output_file_len == final_output_file_len


def test_meltano_error_logging_no_traceback():
    """Logs should produce simple generic error when no traceback."""
    os.environ["MELTANO_PROJECT_ROOT"] = "/"

    with pytest.raises(Exception) as e:
        execute_solid(
            MeltanoEltSolid("csv_to_jsonl").solid,
            input_values={
                "elt_args": {
                    "tap": "tap-csv",
                    "target": "target-jsonl",
                    "job_id": "tap-csv-target-jsonl",
                },
            },
        )

    os.environ["MELTANO_PROJECT_ROOT"] = (Path(__file__).parents[2] / "meltano").__str__()

    assert e.value.args[0] == "The meltano elt failed with code 1. See logs for more information."
    assert not re.search("traceback", e.value.args[0], re.IGNORECASE)


def test_meltano_error_logging_traceback():
    """Error logs should produce traceback."""
    with pytest.raises(Exception) as e:
        execute_solid(
            MeltanoEltSolid("csv_to_jsonl").solid,
            input_values={
                "elt_args": {
                    "tap": "tap-csv",
                    "target": "target-json",
                    "job_id": "tap-csv-target-jsonl",
                },
            },
        )

    assert re.search("traceback", e.value.args[0], re.IGNORECASE)
    assert re.search("most recent call last", e.value.args[0], re.IGNORECASE)  # from true traceback
    assert re.search("Loader 'target-json' is not known to Meltano", e.value.args[0], re.IGNORECASE)


def test_meltano_elt_solid_select():
    """Should run select command"""
    output_filepath = Path(os.environ["MELTANO_PROJECT_ROOT"]) / "load" / "sample.jsonl"
    reset_output_file(output_filepath)

    result = execute_solid(
        MeltanoEltSolid("csv_to_jsonl").solid,
        run_config={"solids": {"csv_to_jsonl": {"config": {"full_refresh": True}}}},
        input_values={
            "elt_args": {"tap": "tap-csv", "target": "target-jsonl"},
            "select_patterns": [["sample", "id"], ["sample", "first_name"]],
        },
    )

    output = read_output_file(output_filepath)
    reset_output_file(output_filepath)

    # clean up
    meltano_select = _create_meltano_select(
        {"patterns": [["--rm", "sample", "id"], ["--rm", "sample", "first_name"]]}
    )
    context = build_solid_context()
    meltano_select.run_select_commands(context.log)

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert output == [
        {"id": "1", "first_name": "peter"},
        {"id": "2", "first_name": "jan"},
        {"id": "3", "first_name": "flim"},
    ]
