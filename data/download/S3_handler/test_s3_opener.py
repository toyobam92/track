from S3_handler.reader import read_csv
import pandas as pd
df = read_csv('s3://track-project/processed/combined/reviews_combine_02_08.csv', load=False)
temp = pd.concat([df.head(), df.tail()])
