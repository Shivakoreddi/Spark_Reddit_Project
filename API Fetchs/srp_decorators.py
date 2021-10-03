from reddit_access import Reddit_access
import requests
import logging
import json
import csv

def request_execute(url,headers):
    response = requests.get(url, headers=headers)
    return response


def csv_to_json():
    data = {}
    def converter(file_csv,file_json):
        with open(file_csv,encoding='utf-8') as f:
            csv_schema = csv.DictReader(f)
            for row in csv_schema:
                key = row['id']
                data[key]=row
        with open(file_json, 'w', encoding='utf-8') as f1:
            f1.write(json.dumps(data, indent=4))
            # f1.close()

        with open(file_json) as f:
            schema = json.load(f)
        return schema
    return converter
