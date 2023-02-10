# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 18:33:56 2023

@author: adewu
"""

#pip install smart-open[s3] boto3
from smart_open import open
import pandas as pd
import boto3
import os
import dotenv
dotenv.load_dotenv()


def read_csv(filepath, load=True, chunksize=4000):    

    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    tp = {'client': session.client('s3')}

    with open(filepath, 'r', encoding='utf-8', transport_params=tp) as fp:
        reader = pd.read_csv(fp, chunksize=chunksize)
        return reader.read() if load else reader
        



if __name__=='__main__':
    file= 's3://track-project/processed/combined/reviews_combine_02_08.csv'
    df = read_csv(file)