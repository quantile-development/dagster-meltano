from dagster_meltano.meltano_elt import MeltanoELT


# Default parameters supplied to MeltanoELT
meltano_elt_params = {
    "tap": "tap-csv",
    "target": "target-json",
    "job_id": "tap-csv-target-json",
    "full_refresh": True,
}


def _create_meltano_elt(full_refresh: bool = True) -> MeltanoELT:
    """This function creates an instance of the MeltanoELT class. Is used by the testing functions.

    Args:
        full_refresh (bool, optional): Whether to create a full refresh command. Defaults to True.

    Returns:
        MeltanoELT: The instance of the MeltanoELT class
    """
    # Inject the full fresh parameter
    meltano_elt_params["full_refresh"] = full_refresh

    # Create the instance
    return MeltanoELT(**meltano_elt_params)


def test_meltano_elt_construction():
    """
    On successfull creation no errors should be raised.
    """
    meltano_elt = _create_meltano_elt()

    # Test if the instance is of the right type
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


def test_meltano_elt_command():
    """
    Test if the generated meltano elt command is of the correct format.
    """
    meltano_elt = _create_meltano_elt()

    # Test if the command is of the right format
    assert meltano_elt.elt_command == [
        "meltano",
        "elt",
        meltano_elt_params["tap"],
        meltano_elt_params["target"],
        "--job_id",
        meltano_elt_params["job_id"],
        "--full-refresh",
    ]


def test_meltano_elt_command_no_refresh():
    """
    Test whether the '--full-refresh' flag is omitted if no full refresh is requested.
    """
    meltano_elt = _create_meltano_elt(full_refresh=False)

    # Make sure that the full refresh flag is missing
    assert "--full-refresh" not in meltano_elt.elt_command
