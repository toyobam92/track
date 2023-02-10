import pandas as pd
from smart_open import open
import boto3
import os
import dotenv

dotenv.load_dotenv()


full_df = pd.DataFrame()

def proc_data_upload(data, output, session):
    with open(output, 'w', transport_params={'client': session.client('s3')}, encoding="utf-8") as f:
        f.write(data)
    return output

def proc_data_download(path, session):
    with open(path, 'r', transport_params={'client': session.client('s3')}, encoding="utf-8") as f:
        try:
            return pd.read_csv(f)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=["updated.label", "im:rating.label", "im:version.label", "title.label", "content.label"])
        
session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
)

s3c = session.client('s3')
resp = s3c.list_objects_v2(Bucket='track-project', Prefix='processed/rss')
TO_PROCESS = [f's3://track-project/{i["Key"]}' for i in resp['Contents'] if i['Key'].endswith('.csv')]


for file in TO_PROCESS:
    df = proc_data_download(file, session)[["updated.label", "im:rating.label", "im:version.label", "title.label", "content.label"]]
    df['date'] = pd.to_datetime(df['date'])
    df.rename(columns={"updated.label": "date", 
                        "title.label": "title", 
                        "im:rating.label":"rating", 
                        "im:version.label": "version",
                        "content.label": "review"}, inplace=True)
    det = file.split('/')[-1].split('-')
    df['file_name'] = det[0]
    df['appid'] = det[1].replace('.csv', '')
    df['method'] = file.split('processed/')[1].split('/')[0]
    full_df = pd.concat([full_df, df])

proc_data_upload(full_df.to_csv(index=False), file.split('year')[0]+'reviewed_02_06.csv', session)


with open('s3://track-project/processed/rss/reviewed_02_06.csv', 'r', encoding='utf-8') as rss, open('s3://track-project/processed/scraped data/reviews_data_01_28.csv', 'r', encoding='utf-8') as scraped:
    rss_df = pd.read_csv(rss)
    scr_df = pd.read_csv(scraped)

scr_df['method'] = 'scraped'
scr_df['date'] = pd.to_datetime(scr_df['date'], format='%D')
out_df = pd.concat([scr_df, rss_df])
proc_data_upload(out_df.to_csv(index=False),'s3://track-project/processed/combined/reviews_combine_02_08.csv', session)    
