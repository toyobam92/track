import json
import pandas 



class Processor:
    def __init__(self, app_list=None):
        if not app_list:
            with open('../DATA/app_list.json', mode='r', encoding='utf-8') as f:
                app_list= json.load(f)
        self.app_list = app_list


    def deduper(self, item, method="id"):
        if method=="nested":
            unique = {json.dumps(d, sort_keys=True) for d in item}
            return [json.loads(d) for d in unique]
        elif method=="id":
            return list({i['id']['label']:i for i in reversed(item)}.values())
            
    def process(self, file_type='rss'):
        if file_type=='rss':
            self.rss_processor()
        elif file_type=='scraper':
            self.scraper_processor()
    
    def scraper_processor(self, item):
        app_reviews_df= pd.DataFrame(np.array(app_store.reviews),columns=['review'])
        app_reviews_df= app_reviews_df.join(pd.DataFrame(app_reviews_df.pop('review').tolist()))
        app_reviews_df.to_csv(f'{app_name}_reviews.csv', index=False)

    def rss_processor(self, item):
        pass