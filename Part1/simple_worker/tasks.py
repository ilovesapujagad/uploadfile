import requests
import csv
import pandas as pd
from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery('tasks', broker='redis://10.10.66.193:6379/0', backend='redis://10.10.66.193:6379/0')

def excel_to_json(file_path, start_row, end_row):
    df = pd.read_excel(file_path, header=None)  # Mengabaikan baris header
    data = []
    count = 0
    for _, row in df.iterrows():
        count += 1
        if count >= start_row and count <= end_row:
            data.append({
                "message": ','.join(['"{0}"'.format(cell) for cell in row])
            })
        if count == end_row:
            break
    return data

def check_file(file,indexname):
    # Baca file CSV dan proses datanya
    df = pd.read_excel(file)
    data = df.to_csv(index=False, quoting=csv.QUOTE_ALL)
    data = data
    
    url = "http://10.10.66.193:5601/internal/file_data_visualizer/analyze_file"
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Host": "10.10.66.193:5601",
        "Origin": "http://10.10.66.193:5601",
        "Referer": "http://10.10.66.193:5601/app/home",
        "kbn-xsrf": "true",
        "X-Kbn-Context": "%7B%22type%22%3A%22application%22%2C%22name%22%3A%22home%22%2C%22url%22%3A%22%2Fapp%2Fhome%22%7D"
    }

    response = requests.post(url,headers=headers, json=data)
    url = "http://10.10.66.193:5601/internal/file_upload/import"
    payload = {
        "index": indexname,
        "data": [],
        "settings": {
            "number_of_shards": 1
        },
        "mappings": {
            "properties": response.json()['results']["mappings"]["properties"]
        },
        "ingestPipeline": {
            "id": indexname+"-pipeline",
            "pipeline": { 
            "description": "Ingest pipeline created by text structure finder",
            "processors": response.json()['results']["ingest_pipeline"]["processors"]
            }
        }
    }
    payload["mappings"]["properties"]["location"] = {"type": "geo_point"}
    payload["ingestPipeline"]["pipeline"]["processors"].append({
    "set": {
        "field": "location",
        "value": "{{Lat}},{{Lon}}"
        }
    })
    response = requests.post(url,headers=headers, json=payload)
    return response.json()

@app.task()
def process_task(file,indexname):
    id = check_file(file,indexname)
    
    df = pd.read_excel(file)
    row_count = len(df)
        # data = df.to_csv(index=False, quoting=csv.QUOTE_ALL)
    url = "http://10.10.66.193:5601/internal/file_upload/import?id="+id["id"]
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Host": "10.10.66.193:5601",
        "Origin": "http://10.10.66.193:5601",
        "Referer": "http://10.10.66.193:5601/app/home",
        "kbn-xsrf": "true",
        "X-Kbn-Context": "%7B%22type%22%3A%22application%22%2C%22name%22%3A%22home%22%2C%22url%22%3A%22%2Fapp%2Fhome%22%7D"
    }

    start_row = 2
    end_row = 25502
    while True:
        x = excel_to_json(file, start_row, end_row)
        payload = {
            "index": indexname,
            "data": x,
            "settings": {},
            "mappings": {},
            "ingestPipeline": {
                "id": indexname+"-pipeline"
            }
        }
        response = requests.post(url,headers=headers, json=payload)
        if end_row+1 >= row_count:  
            break
        if response.status_code != 200:
            return {'row error': end_row,"status": response.status_code}
        print(end_row)
        start_row = end_row+1
        end_row += 25500


