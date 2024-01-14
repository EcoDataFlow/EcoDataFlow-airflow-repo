import pytest
import sys
import os


def run_tests():
    result = pytest.main(["-v", "-k", "test_", "tests"])

    sys.exit(result)


if __name__ == "__main__":
    # 현재 스크립트 파일의 디렉토리 경로를 얻음
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 상대 경로를 이용하여 필요한 경로를 계산
    tests_dir = os.path.join(current_dir, "../../tests")
    dags_dir = os.path.join(current_dir, "../../dags")

    # 현재의 sys.path 출력
    print("현재 모듈 검색 경로:", sys.path)

    # 계산된 경로를 sys.path에 추가
    sys.path.append(tests_dir)
    sys.path.append(dags_dir)

    # 수정 후의 sys.path 출력
    print("수정된 모듈 검색 경로:", sys.path)

    run_tests()
