import json
import pandas as pd
import numpy as np 
from smart_open import open

def proc_data_upload(data, output, session):
    with open(output, 'w', transport_params={'client': session.client('s3')}, encoding="utf-8") as f:
        f.write(data)
    return output

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
            
    def process(self, data, filename, session):
        output_path = filename.replace('/raw/', '/processed/').replace('.json', '.csv')
        if filename.split('/raw/')[1][:3]=='rss':
            output = self.rss_processor(data)
        elif filename.split('/raw/')[1][:3]=='scraper':
            output = self.scraper_processor(data)
        
        print(f"Processing data from: {filename.split('/')[-1]} ")
        proc_data_upload(output, output_path, session)
        print(f"File uploaded into: {output_path}\n")

 
    
    def scraper_processor(self, data):
        app_reviews_df= pd.DataFrame(np.array(data),columns=['review'])
        app_reviews_df= app_reviews_df.join(pd.DataFrame(app_reviews_df.pop('review').tolist()))
        return app_reviews_df.to_csv(index=False)

    def rss_processor(self, data):
        temp = pd.json_normalize(data)[["updated.label", "im:rating.label", "im:version.label", "title.label", "content.label"]]
        return temp.to_csv(index=False)
    
    
#     df = pd.json_normalize(data)




# df[df['author.name.label'].apply(lambda x: 'Tea' in x)]
