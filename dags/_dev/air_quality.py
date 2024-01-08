import requests
import pandas as pd

url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
params = {
    "serviceKey": "9IyndkiMrrzo5eLkP+I/sKhMYeg0jb8hNwqpdPHdeRKS5WuCsdT/bA8urOBesACx9E9cmdhLVs9sDvAFiyVlsA==",
    "returnType": "json",
    "numOfRows": "1000",
    "pageNo": "1",
    "sidoName": "전국",
    "ver": "1.0",
}

response = requests.get(url, params=params)
# print(response.content)
json_data = response.json()["response"]["body"]["items"]

# print(json_data.keys())

df = pd.DataFrame(json_data)

df.to_csv("output.csv", index=False)
