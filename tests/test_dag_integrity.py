import importlib
import os


def test_dag_integrity():
    dag_directory = 'dags/'

    dag_files = [os.path.join(dag_directory, file) for file in os.listdir(dag_directory) if file.endswith('.py')]

    for dag_file in dag_files:
        module_name = dag_file.replace('/', '.').replace('.py', '')

        try:
            importlib.import_module(module_name)
        
        except ImportError as e:
            assert False, f"ImportError in {dag_file}: {e}"
