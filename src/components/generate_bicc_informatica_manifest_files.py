#Lambda code to generate manifest file required for Informatica mapping for BICC file load
import json
import os
from datetime import datetime
import boto3

mapping_json_path=os.environ['mapping_file_path']
s3_bucket=os.environ['bucket']
client = boto3.client('s3')

def lambda_handler(event, context):
    
    ### Downloading the mapping 
    download_file(s3_bucket,mapping_json_path+'mapping.json','/tmp/mapping.json')
    json_file_path = '/tmp/mapping.json'
    with open(json_file_path, "r") as json_file:
        json_data = json.load(json_file)

    current_month = datetime.now().strftime('%m')
    current_year = datetime.now().strftime('%Y')
    dtvalue=str(current_year)+str(current_month)
    ##Generating Manifest files
    for key in json_data.keys():
        parent= json_data[key]['parent']
        level1= json_data[key]['level1']
        table= json_data[key]['table']
        create_manifest_file(parent,level1,dtvalue,table)
    os.remove(json_file_path)


def upload_file(bucket, localpath, s3path):
    client.upload_file(localpath, bucket, s3path)

def download_file(bucket,s3path,downloadpath):
    print(bucket+s3path+downloadpath)
    client.download_file(bucket,s3path,downloadpath)

def create_manifest_file(parent,level1,dtvalue,table):
    manifest_data="""{"fileLocations": [{"WildcardURIs": ["%s%s/%s/%s/extracted_files/*.csv"]}],"settings": {"stopOnFail": "true"}}""" %(parent, dtvalue,level1,table)
    #print(manifest_data)
    manifest_file_path = '/tmp/'+table+'.manifest'
    with open(manifest_file_path, "w") as manifest_file:
         manifest_file.write(manifest_data+'\n')
    s3_path=parent+'manifest/'+table+'.manifest'
    upload_file(s3_bucket,manifest_file_path,s3_path)
    os.remove(manifest_file_path)