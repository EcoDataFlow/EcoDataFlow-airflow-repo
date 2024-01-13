# test_dag_integrity.py
import pytest
import glob
import importlib.util
from airflow.models import DAG, Variable
from airflow.utils.dag_cycle_tester import check_cycle
from pathlib import Path

DIR = Path(__file__).parents[0]
DAG_PATH = DIR / ".." / ".." / "dags/"
DAG_FILES = glob.glob(str(DAG_PATH / '**/*.py'), recursive=True)

def import_dag_files(dag_path, dag_file):
    module_name = Path(dag_file).stem
    module_path = dag_path / dag_file
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    return module

def test_dag_integrity(airflow_variables, monkeypatch):
    # Airlfow variables monkey patch
    def mock_get(*args, **kwargs):
        mocked_dict = airflow_variables
        return mocked_dict.get(args[0])

    monkeypatch.setattr(Variable, "get", mock_get)

    for dag_file in DAG_FILES:
        module = import_dag_files(DAG_PATH, dag_file)
        dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
        assert dag_objects

        for dag in dag_objects:
            check_cycle(dag)
