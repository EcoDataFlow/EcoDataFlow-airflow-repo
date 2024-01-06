import subprocess
import os

def test_dependencies():
    try:
        # requirements.txt 파일의 경로
        requirements_txt_path = os.path.join(os.getcwd(), 'requirements.txt')
        
        # requirements.txt 파일을 읽어와서 의존성 리스트 생성
        with open(requirements_txt_path, 'r') as f:
            required_dependencies = [line.strip() for line in f.readlines() if line.strip()]

        for dependency in required_dependencies:
            try:
                subprocess.run(['pip', 'show', dependency], check=True)
                assert True 
            except subprocess.CalledProcessError:
                assert False, f"Dependency import failed: {dependency}"
    except FileNotFoundError:
        assert False, "requirements.txt file not found"
