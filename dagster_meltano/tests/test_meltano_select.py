import os
import re
from pathlib import Path
from typing import Optional

from dagster import build_solid_context

from dagster_meltano.meltano_select import MeltanoSelect
from dagster_meltano.tests.utils import read_output_file

os.environ["MELTANO_PROJECT_ROOT"] = (Path(__file__).parents[2] / "meltano").__str__()

# Default parameters supplied to MeltanoELT
meltano_elt_params = {
    "tap": "tap-csv",
    "target": "target-json",
}


def _create_meltano_select(select_args: Optional[dict] = None) -> MeltanoSelect:
    """This function creates an instance of the MeltanoSelect class. Is used by the testing functions.

    Args:
        select_args (dict, optional): Dictionary containing all relevant arugments to test.

    Returns:
        MeltanoSelect: The instance of the MeltanoSelect class.
    """
    # Inject the additional args
    params = {**meltano_elt_params, **select_args}

    # Create the instance
    return MeltanoSelect(**params)


def test_meltano_select_command_list_opt_current():
    """Test if the generated meltano select command is of the correct format."""
    meltano_select = _create_meltano_select({})

    assert meltano_select.select_command(["this", "is", "a", "test"]) == [
        "meltano",
        "select",
        meltano_elt_params["tap"],
        "this",
        "is",
        "a",
        "test",
    ]


def test_meltano_select_run_select_commands_patterns():
    """Test if the generated meltano select command is of the correct format."""
    meltano_select = _create_meltano_select(
        {"patterns": [["test", "this"], ["--remove", "test", "this"]]}
    )
    context = build_solid_context()
    res = meltano_select.run_select_commands(context.log)

    output_filepath = Path(os.environ["MELTANO_PROJECT_ROOT"]) / "meltano.yml"
    meltano_yml = read_output_file(output_filepath, json_lines=False)

    assert res[0] == ["meltano", "select", meltano_elt_params["tap"], "test", "this"]
    assert res[1] == ["meltano", "select", meltano_elt_params["tap"], "--remove", "test", "this"]
    assert not re.search(r"test\.this", meltano_yml)  # make sure the second pattern actually ran
