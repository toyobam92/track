import pandas as pd
import numpy as np
import json
import time
import requests
from app_store_scraper import AppStore
import pathlib
from datetime import datetime, timedelta
from smart_open import open

now = datetime.now()
params = {
    'now_day': str(now.day),
    'now_day_02': f"{now.day:02d}",
    'now_month': str(now.month),
    'now_month_02': f"{now.month:02d}",
    'now_month_text': now.strftime('%B'),
    'now_year': str(now.year)
}


def parameterise_input(inp):
    for i in params: inp=inp.replace("${"+f'{i}'+"}",params[i])
    return inp
def data_upload(data, output):
    with open(output, 'wb') as f:
        json.dump(data, f)
    return output


def get_reviews_data(conf, bucket):
    ext = Extractor(conf["app"])
    for app in conf["app"]:
        filename, data = ext.get_reviews_rss(app)
        output_path = f"s3://{bucket}/{parameterise_input(conf['output']+filename)}.json"

        print(f"Downlaoding data for: {app} ...")
        data_upload(data, output_path)
        print(f"File uploaded into: {output_path}\n")




class Extractor:
    def __init__(self, app_list:dict=None):
        self.session = requests.Session()
        self.app_list = app_list


    def check_for_id(self, app_name):
        if app_id := self.app_list.get(app_name):
            return app_id
        else:
            raise ValueError("App ID not found!")
    
        
    def save_reviews(self, item, name):
        data = pathlib.Path('../DATA/Reviews')
        if curr:=list(data.glob(name)):
            name = f'{name}-v{len(curr)}'
        with open(data/f'{name}.json', 'w') as f:
            json.dump(item, f, indent=4)

    def check_response(self, response):
        if response.status_code!=200:
            raise Exception({"status": response.status_code})
        return response.json()['feed']
        
    def get_reviews_rss(self, app_name="Acorns", number_of_reviews=2000, save='s3'):
        app_id = self.check_for_id(app_name)
        pages = min(10, number_of_reviews//50)
        all_reviews = []
        for page in range(1, pages+1):      
            base_url = f'https://itunes.apple.com/us/rss/customerreviews/id={app_id}/sortBy=mostRecent/page={page}/json'
            response = self.session.get(base_url)
            if entry:=self.check_response(response).get('entry'):
                all_reviews.extend(entry)
            else:
                print(f"No entry found in response at page {page}!")
                break
        if save=="local":
            self.save_reviews(all_reviews, f'{app_name}-{app_id}-rss'.lower())
        elif save=='s3':
            return f'{app_name}-{app_id}-rss'.lower(), all_reviews
            
                    
        
    def get_reviews_scraper(self, app_name="Acorns", number_of_reviews=2000, no_of_days=None): 
        try:           
            self.attempt_get_reviews(app_name, no_of_days, number_of_reviews)
        except ValueError as e :
            return print(e)
        
        except Exception as e:
            print(f"{app_name} not found, retrying after 120 seconds")
            print(e)
            time.sleep(60)

    def attempt_get_reviews(self, app_name, no_of_days, number_of_reviews, save='s3'):
        app_id = self.check_for_id(app_name)
        if no_of_days:
            after = datetime.now() - timedelta(no_of_days)
        app_store = AppStore(country='us', app_name=app_name, app_id = app_id)
        app_store.review(how_many=number_of_reviews, after=after)
        if save=='local':
            self.save_reviews(app_store.reviews, f'{app_name}-{app_id}-scraper'.lower())
        elif save=='s3':
            return f'{app_name}-{app_id}-rss'.lower(), app_store.reviews
          

if __name__=='__main__':
    with open('../DATA/app_list.json', mode='r', encoding='utf-8') as f:
        apps =  json.load(f) 
    
    event = {
            'app': apps,
            'output': 'raw/year=${now_year}/month=${now_month}/day=${now_month_text}/day=${now_day_02}/'
        }
    
    start_time = time.perf_counter()
    get_reviews_data(event, 'track-project')
    end_time = time.perf_counter()
    print(f"Time elapsed is {end_time - start_time}\n")