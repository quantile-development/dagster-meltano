import pytest
from dagster_meltano.meltano_elt import MeltanoELT

def test_meltano_elt_construction():
    """On successfull creation no errors should be raised.
    """    
    meltano_elt = MeltanoELT(
        tap="tap-csv",
        target="target-csv",
        job_id="tap-csv-target-json",
        full_refresh=True
    )

    assert isinstance(meltano_elt, MeltanoELT)

def test_meltano_elt_missing_params():
    """An error should be raised when the construction params
    are missing.
    """    
    with pytest.raises(TypeError):
        MeltanoELT()
