import pytest
import glob
import importlib.util
from airflow.models import DAG, Variable
from airflow.utils.dag_cycle_tester import check_cycle
from pathlib import Path

DIR = Path(__file__).parents[0]
DAG_PATH = DIR / ".." / ".." / "dags/**.py"
DAG_FILES = glob.glob(str(DAG_PATH))

def import_dag_files(dag_path, dag_file):
    module_name = Path(dag_path).stem
    module_path = dag_path / dag_file
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    print(module)

    return module

@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(airflow_variables, dag_file, monkeypatch):
		# Airlfow variables monkey patch
    def mock_get(*args, **kwargs):
        mocked_dict = airflow_variables
        return mocked_dict.get(args[0])

    monkeypatch.setattr(Variable, "get", mock_get)

    module = import_dag_files(DAG_PATH, dag_file)
    dag_objects = dag_objects = [getattr(module, var) for var in dir(module) if isinstance(getattr(module, var), DAG)]

    assert dag_objects

    for dag in dag_objects:
        check_cycle(dag)
