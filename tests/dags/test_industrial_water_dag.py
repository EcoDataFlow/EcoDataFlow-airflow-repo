# from dags.water.industrial import get_water_purification_plants_info, industrial_water_dag
# from dags.water.industrial.industrial_water_dag import *
#
# import requests
#
#
# class TestWaterDAG:
#     def setUp(self):
#         self.url = "http://apis.data.go.kr/B500001/waterways/wdr/waterfltplt/waterfltpltlist"
#         self.params = {
#             "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
#             "numOfRows": "100",
#             "pageNo": "1",
#             "_type": "json",
#         }
#
#
#     def test_water_plants_response_data(self):
#         print(requests.get(self.url, params=self.params).json()["response"]["body"]["items"])
#
#
#     def test_get_water_purification_plants_info(self):
#         # num_of_pages 변수 로직도 에러 안일으키고 맞음! 출력까지 해서 확인함
#         get_water_purification_plants_info().to_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/water/water_plants.csv", index=False)
#
#
#     def test_load_water_plants(self):
#         import csv
#         with open("/dags/water/industrial/water_plants.csv",
#                   'r') as csvfile:
#             reader = csv.reader(csvfile)  # Assuming the first row contains the column headers
#             water_plants = {row[0]: row[1] for row in reader}
#             return water_plants
#
#
#     def test_daily_industrial_water_response_data(self):  # 뭘 체크하고자 하는지 알려주면 더 좋을듯
#         get_industrial_water_quality_infos()  # 또 string indices must be int, not str() 이거 뜸. 더 정교한 검사 필요
#
#
#     def test_water_plants_info_row(self):
#         water_plants_info = pd.read_csv("/dags/data/industrial_water/new_water_plant_addresses.csv")
#         for i in range(0, water_plants_info.shape[0]):
#             print(water_plants_info.iloc[i])