import datetime
import json
import random
import boto3
import time

STREAM_NAME = "customer"

# Dictionary to map customer IDs to customer names
CUSTOMER_MAP = {
    1: 'NICK',
    2: 'EDEN',
    3: 'GRACE',
    4: 'MATTHEW',
    5: 'UMA',
    6: 'KARL',
    7: 'FRED',
    8: 'HELEN',
    9: 'DAN',
    10: 'BOB'
}

def get_data():
    customer_id = random.randint(1, 10)
    customer_name = CUSTOMER_MAP[customer_id]
    email_id = customer_name.lower() + "@gmail.com"
    country_id = customer_id
    return {
        'customerid': customer_id,
        'customername': customer_name,
        'emailid': email_id,
        'countryid': country_id
    }

def generate(stream_name, kinesis_client):
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=str(data['customerid']))
        time.sleep(1)

if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))

