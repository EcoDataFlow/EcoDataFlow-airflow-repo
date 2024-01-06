import pytest
import sys

def run_tests():
    result = pytest.main(['-v', '-k', 'test_', 'tests'])

    sys.exit(result)

if __name__ == '__main__':
    run_tests()
