import json
import os
from pathlib import Path

from dagster import AssetMaterialization, execute_solid

from dagster_meltano.solids import meltano_elt_solid


def test_meltano_elt_solid_explicit_params():
    result = execute_solid(
        meltano_elt_solid,
        input_values={
            "elt_args": {
                "name": "csv",
                "tap": "tap-csv",
                "target": "target-jsonl",
                "job_id": "tap-csv-target-json",
            },
        },
    )

    mtrls = result.materializations_during_compute
    mtrl = mtrls[0]

    metadata = {}
    for entry in mtrls[0].metadata_entries:
        metadata[entry.label] = entry.entry_data.text

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(mtrl, AssetMaterialization)
    assert mtrl.asset_key[0][0] == "csv"
    assert metadata["Tap"] == "tap-csv"
    assert metadata["Target"] == "target-jsonl"
    assert metadata["Job ID"] == "tap-csv-target-json"
    assert metadata["Full Refresh"] == "False"


def test_meltano_elt_solid_nonexplicit_params():
    """Auto-generated inputs values match requirements."""
    result = execute_solid(
        meltano_elt_solid,
        input_values={
            "elt_args": {
                "tap": "tap-csv",
                "target": "target-jsonl",
            },
        },
    )

    mtrls = result.materializations_during_compute
    mtrl = mtrls[0]

    metadata = {}
    for entry in mtrls[0].metadata_entries:
        metadata[entry.label] = entry.entry_data.text

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(mtrl, AssetMaterialization)
    assert mtrl.asset_key[0][0] == "tap_csv_target_jsonl"
    assert metadata["Tap"] == "tap-csv"
    assert metadata["Target"] == "target-jsonl"
    assert metadata["Job ID"] == "tap-csv-target-jsonl"
    assert metadata["Full Refresh"] == "False"


def test_meltano_elt_solid_env_vars():
    """Config env vars should override input provided env vars."""
    output_filepath = Path(os.environ["MELTANO_PROJECT_ROOT"]) / "load" / "sample.jsonl"

    def len_output_file(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            file = f.read()
        return len(file)

    initial_output_file_len = len_output_file(output_filepath)

    result = execute_solid(
        meltano_elt_solid,
        run_config={
            "solids": {
                "meltano_elt_solid": {
                    "config": {"env_vars": {"TAP_CSV__SELECT": json.dumps(["!sample.*"])}}
                }
            }
        },
        input_values={
            "elt_args": {
                "tap": "tap-csv",
                "target": "target-jsonl",
                "env_vars": {"TAP_CSV__SELECT": json.dumps(["test_value"])},
            },
        },
    )

    final_output_file_len = len_output_file(output_filepath)

    mtrls = result.materializations_during_compute
    mtrl = mtrls[0]

    metadata = {}
    for entry in mtrls[0].metadata_entries:
        metadata[entry.label] = entry.entry_data.text

    assert result.success
    assert len(result.materializations_during_compute) == 1
    assert isinstance(mtrl, AssetMaterialization)
    assert mtrl.asset_key[0][0] == "tap_csv_target_jsonl"
    assert metadata["Tap"] == "tap-csv"
    assert metadata["Target"] == "target-jsonl"
    assert metadata["Job ID"] == "tap-csv-target-jsonl"
    assert metadata["Full Refresh"] == "False"
    assert initial_output_file_len == final_output_file_len
