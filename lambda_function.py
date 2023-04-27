import json
import base64
import csv
import requests
import boto3
from botocore.errorfactory import ClientError

def lambda_handler(event, context):
    # TODO implement
    print("Night")
    finaldata=[]
    s3 = boto3.resource('s3')
    
    bucket = s3.Bucket('585youtubecode')
    key = 'youtubeoutput.csv'
    local_file_name = '/tmp/youtubeoutput.csv'
    try:
        # s3.head_object(Bucket='585youtubecode', Key=key)
        bucket.download_file(key,local_file_name)
    except Exception as e:
        with open('/tmp/youtubeoutput.csv','w+') as infile:
            print("Creating file")
    
            
    for record in event["Records"]:
        decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        finaldata.append(decoded_data)
    with open('/tmp/youtubeoutput.csv','r') as infile:
         reader = list(csv.reader(infile))
         reader.insert(0,finaldata)
        
    with open('/tmp/youtubeoutput.csv', 'w+', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reader:
             writer.writerow(line)
    bucket.upload_file(local_file_name, key)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
