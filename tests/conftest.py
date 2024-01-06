import pytest

import sys

assert (
    "airflow" not in sys.modules
), "No airflow module can be imported before these lines"

@pytest.fixture(autouse=True)
def airflow_variables():
    return {
        "variables1": "abc",
        "variables2": "abc",
        "variables3": "abc",
        "variables4": "abc",		
    }
