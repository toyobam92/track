import pandas as pd
import numpy as np
import json
import time

from smart_open import open
import os, boto3
from datetime import datetime
from dotenv import load_dotenv

from extractor import Extractor 
from processor import Processor
load_dotenv()
now = datetime.now()
params = {
    'now_day': str(now.day),
    'now_day_02': f"{now.day:02d}",
    'now_month': str(now.month),
    'now_month_02': f"{now.month:02d}",
    'now_month_text': now.strftime('%B'),
    'now_year': str(now.year)
}

TO_PROCESS = []


def parameterise_input(inp):
    for i in params: inp=inp.replace("${"+f'{i}'+"}",params[i])
    return inp
def data_upload(data, output, session):
    with open(output, 'w', transport_params={'client': session.client('s3')}) as f:
        json.dump(data, f)
    return output


def get_reviews_data(conf, bucket, session):
    ext = Extractor(conf["app"])
    for app in conf["app"]:
        filename, data, method = ext.get_reviews_rss(app)
        output_path = f"s3://{bucket}/raw/{method}/{parameterise_input(conf['output']+filename)}.json"

        print(f"Downlaoding data for: {app} ")
        data_upload(data, output_path, session)
        print(f"File uploaded into: {output_path}\n")
        TO_PROCESS.append(output_path)



def process_reviews_data(files, session):
    proc = Processor()    
    for filename in files:
        with open(filename, 'r', transport_params={'client': session.client('s3')}) as f:
            data = json.load(f)
            proc.process(data, filename, session)


if __name__=='__main__':
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    with open('../DATA/app_list.json', mode='r', encoding='utf-8') as f:
        apps =  json.load(f) 
    
    event = {'app': apps, 'output': 'year=${now_year}/month=${now_month_text}/day=${now_day_02}/'}
    start_time = time.perf_counter()
    get_reviews_data(event, 'track-project', session)
    end_time = time.perf_counter()
    print(f"Extraction: Time elapsed is {end_time - start_time}\n")
    
    # if not TO_PROCESS:
    #     s3c = session.client('s3')
    #     resp = s3c.list_objects_v2(Bucket='track-project', Prefix='raw/')
    #     TO_PROCESS = [f's3://track-project/{i["Key"]}' for i in resp['Contents'] if i['Key'].endswith('json')]
 
    start_time = time.perf_counter()   
    process_reviews_data(TO_PROCESS, session)
    end_time = time.perf_counter()
    print(f"Processing: Time elapsed is {end_time - start_time}\n")
