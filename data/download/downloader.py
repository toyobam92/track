import os, requests, time
from datetime import datetime
from smart_open import open

now = datetime.now()
params = {'now_day': str(now.day),
 'now_day_02': f"{now.day:02d}",
 'now_month': str(now.month),
 'now_month_02': f"{now.month:02d}",
 'now_month_text': now.strftime('%B'),
 'now_year': str(now.year)
}

def parameterise_input(inp):
    for i in params: inp=inp.replace("${"+f'{i}'+"}",params[i])
    return inp

def download_file(url, output):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output, 'wb') as f:
            for chunk in r.iter_content(chunk_size=100000): f.write(chunk)
    return output

def run_download(conf, bucket):
    url = parameterise_input(conf['url'])
    output = f"s3://{bucket}/{parameterise_input(conf['output'])}"
    print(f"Downlaoding file from: {url} ...")
    download_file(url, output)
    print(f"File downloaded as: {output}\n")


## EOF ##
# Example of how to use function
if __name__ == "__main__":
    bucket = f"doctrina-data-{os.environ['STAGE']}-{os.environ['AWS_ACCOUNT_ID']}"
    event = {
            'url': 'https://download.cms.gov/nppes/NPPES_Data_Dissemination_${now_month_text}_${now_year}.zip',
            'output': 'nppes/year=${now_year}/month=${now_month_02}/NPPES_Data_Dissemination_${now_month_text}_${now_year}.zip'
        }
    start_time = time.perf_counter()
    run_download(event,bucket)
    end_time = time.perf_counter()
    print(f"Time elapsed is {end_time - start_time}\n")

    event = {
            'url': 'https://data.cms.gov/data-api/v1/dataset/88bd5fb4-7b5c-4107-8131-23c485e00ef0/data',
            'output': 'taxonomy/CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY.json'
        }
    start_time = time.perf_counter()
    run_download(event,bucket)
    end_time = time.perf_counter()
    print(f"Time elapsed is {end_time - start_time}\n")