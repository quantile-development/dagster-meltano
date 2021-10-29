import pytest

from dagster_meltano.meltano_elt import MeltanoELT

meltano_elt = MeltanoELT(
    tap="tap-csv", target="target-csv", job_id="tap-csv-target-json", full_refresh=True
)


def test_meltano_elt_construction():
    """On successfull creation no errors should be raised."""

    assert isinstance(meltano_elt, MeltanoELT)
    assert meltano_elt.elt_command == [
        "meltano",
        "elt",
        "tap-csv",
        "target-csv",
        "--job_id",
        "tap-csv-target-json",
        "--full-refresh",
    ]


def test_meltano_elt_missing_params():
    """An error should be raised when the construction params
    are missing.
    """
    with pytest.raises(TypeError):
        MeltanoELT()  # pylint: disable=E1120
