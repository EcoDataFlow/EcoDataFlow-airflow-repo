import pytest
import sys


def run_tests():
    result = pytest.main(['test_'])
    
    sys.exit(result)


if __name__=='__main__':
    run_tests()
