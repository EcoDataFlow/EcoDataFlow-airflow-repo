# This logic is used to create water_plants.csv and new_water_plant_addresses.csv
import requests
import pandas as pd


def get_water_purification_plants_info():
    url = "http://apis.data.go.kr/B500001/waterways/wdr/waterfltplt/waterfltpltlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "100",
        "pageNo": "1",
        "_type": "json",
    }

    try:
        response = requests.get(url, params=params).json()["response"]["body"]
        jsons = response["items"]["item"]
        num_of_pages = response["totalCount"] // response["numOfRows"] + 1
        water_plants_df = pd.DataFrame(jsons)
        for i in range(2, num_of_pages + 1):
            i_str = str(i)
            params["pageNo"] = i_str
            temp_df = pd.DataFrame(
                requests.get(url, params=params).json()["response"]["body"]["items"][
                    "item"
                ]
            )
            water_plants_df.append(temp_df)
        return water_plants_df
    except Exception as err:
        print(f"{err}")


def create_joined_plants_and_address_csv():
    import pandas as pd

    plants_df = pd.read_csv(
        "/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/water_plants.csv"
    )
    address_df = pd.read_csv(
        "/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/water_plant_addresses.csv"
    )  # , encoding='utf-8'
    address_df.pop("관리번호")
    address_df.pop("건설일")
    address_df.pop("정수용량")
    address_df.pop("전처리 시설")
    new_df = plants_df.merge(
        address_df, left_on="fltpltnm", right_on="정수장 명", how="inner"
    )
    new_df.pop("정수장 명")
    new_df.to_csv(
        "/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/new_water_plant_addresses.csv",
        index=False,
    )
