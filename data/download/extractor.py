import requests
from app_store_scraper import AppStore
import pathlib
from datetime import datetime, timedelta
import json
import time


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
        print(f'{app_name} => {len(all_reviews)} reviews')
        if save=="local":
            self.save_reviews(all_reviews, f'rss/{app_name}-{app_id}'.lower())
        elif save=='s3':
            return f'{app_name}-{app_id}'.lower(), all_reviews, 'rss'
            
                    
        
    def get_reviews_scraper(self, app_name="Acorns", number_of_reviews=2000, no_of_days=None): 
        try:           
            return self.attempt_get_reviews(app_name, no_of_days, number_of_reviews)
        except ValueError as e :
            return print(e)
        
        except Exception as e:
            print(f"{app_name} not found, retrying after 20 seconds")
            print(e)
            time.sleep(20)

    def attempt_get_reviews(self, app_name, no_of_days, number_of_reviews, save='s3'):
        app_id = self.check_for_id(app_name)
        
        after = datetime.now() - timedelta(no_of_days) if no_of_days else None
        app_store = AppStore(country='us', app_name=app_name, app_id = app_id)
        app_store.review(how_many=number_of_reviews, after=after, sleep=20)
        print(f'{app_name} => {len(app_store.reviews)} reviews\n')
        if save=='local':
            self.save_reviews(app_store.reviews, f'scraper/{app_name}-{app_id}'.lower())
        elif save=='s3':
            return f'{app_name}-{app_id}'.lower(), app_store.reviews, 'scraper'
          



# stream content *into* S3 (write mode) using a custom session

# with open(url, 'wb', transport_params={'client': session.client('s3')}) as fout:
#     bytes_written = fout.write(b'hello world!')
#     print(bytes_written)