from flask import Flask, request, Response
import pandas as pd
import subprocess

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def respond():
    webhook_json = request.json
    
    if webhook_json["action"] == "completed":
        print("Webhook")
        if webhook_json["workflow_run"]["conclusion"] == "success":
            print(webhook_json["workflow_run"]["conclusion"])
            subprocess.call(['git', 'pull','origin','main'], cwd='/home/sarah/devcourse/github-airflow-test')
        else :
            print("Workflow was not successfully completed.")
    return Response(status=200)
    
@app.route("/")
def main():
    return("hello, flask!")
    
if __name__ == "__main__":
    app.run()
