import json
import os
from pathlib import Path

from dagster import AssetMaterialization, execute_solid

from dagster_meltano.solids import MeltanoEltSolid

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

    def len_output_file(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            file = f.read()
        return len(file)

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
