import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np

#api 조회
api = requests.get("http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty?serviceKey=Kp8sABnsI9uXte44um5itP%2BRBcqtw4K7J3zRmT09rwySwX2BhnwvGbtGMVSrjP56IULG8UdLHB%2F7vf4eGbKHUA%3D%3D&returnType=json&numOfRows=100&pageNo=1&stationName=%EB%AC%B8%EB%A7%89%EC%9D%8D&dataTerm=DAILY&ver=1.0")

#파싱
res = api.text
data = json.loads(res)
datas = data["response"]["body"]["items"]
data_list = []

for d in datas:
    pm10 = d["pm10Value"]
    pm25 = d["pm25Value"]
    co = d["coValue"]
    date = d["dataTime"]
    data_list.append([pm10, pm25, co, date])

#DB에 저장
engine = create_engine("mysql+pymysql://root:0000@localhost/crawl_data")
conn = engine.connect()

df = pd.DataFrame(data_list, columns=['p10', 'p25', 'co', 'date'])
df.sort_values(by=['date'], inplace=True)
df.to_csv('text.txt', index=False, sep='\t')
df.to_sql('dust', engine, index=False, if_exists='replace')

#DB에서 조회
rdf = pd.read_sql('dust', engine)
print(rdf)

#그래프 표시 부분
sdf = pd.read_sql('dust', engine)
sdf = sdf.astype(
    {'p10': int,
     'p25': int,
     'co': np.float,
     'date': str
     }
)

y1 = sdf['p10']
y2 = sdf['p25']
y3 = sdf['co']
date = sdf['date'].str.slice(start=8)
x = len(date)

plt.figure(figsize=(18, 8))

plt.subplot(1, 3, 1).set_title("pm10")
plt.bar(date, y1, color='skyblue', align='center')
plt.xticks(rotation=90)

plt.subplot(1, 3, 2).set_title("pm2.5")
plt.bar(date, y2, color='red', align='center')
plt.xticks(rotation=90)

plt.subplot(1, 3, 3).set_title("co")
plt.bar(date, y3, color='green', align='center')
plt.xticks(rotation=90)

plt.show()
