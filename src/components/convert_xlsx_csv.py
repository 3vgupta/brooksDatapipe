### Lambda code to convert excel to CSV
import csv
import pandas as pd
from openpyxl import load_workbook
import boto3
import os
import json
from datetime import datetime


client = boto3.client('s3')


def lambda_handler(event, context):
        if len(event["Records"]) > 1:
            print('Multiple SQS events,'+str(len(event["Records"])))
        for i in range(0, len(event["Records"])):
            s3eventdata = json.loads(event["Records"][i]["body"])
        #print(s3eventdata)
            if len(s3eventdata["Records"]) > 1:
                print('Multiple S3 events,'+str(len(s3eventdata["Records"])))
        for j in range(0, len(s3eventdata["Records"])):
            s3event = s3eventdata["Records"][j]
            s3_bucket_q = s3event['s3']['bucket']['name']
            sourceObjectKey = s3event["s3"]["object"]["key"]
            download_folder=sourceObjectKey[:sourceObjectKey.rfind('/')+1]
            upload_folder=download_folder.replace('excel','csv')
            archive_folder=download_folder.replace('excel','archive')
            bucket=s3_bucket_q
            file_name=sourceObjectKey.replace(download_folder,'').replace('.xlsx','')
            file_process(file_name,upload_folder,archive_folder,download_folder,bucket) 



def file_process(file_name,upload_folder,archive_folder,download_folder,bucket):
    input_xlsx_file = '/tmp/'+file_name+'.xlsx'
    output_csv_file = '/tmp/'+file_name+'.csv'
    print('Downloading file from S3')
    download_file(bucket,download_folder+file_name+'.xlsx',input_xlsx_file)
    print('Download complete')
    sheet_name = "ProcessedData"  # Replace with the name of the sheet you want to convert
    print('Initiating excel to csv')
    xlsx_to_csv(input_xlsx_file, output_csv_file, sheet_name)
    print('excel to csv complete')
    print('Uploading csv file to S3')
    upload_file(bucket,output_csv_file,upload_folder+file_name+'.csv')
    print('Upload completed')
    print('Removing the files from the memory')
    os.remove(input_xlsx_file)
    os.remove(output_csv_file)
    print('moving files to archive folder')
    timevalue=str(datetime.now())
    source_location=download_folder+file_name+'.xlsx'
    target_location=archive_folder+file_name+timevalue+'.xlsx'
    movefile(bucket,source_location,target_location)
    print('Execution completed')
    

def xlsx_to_csv(input_file, output_file, sheet_name):
    try:
        df = pd.read_excel(input_file, sheet_name=sheet_name,engine='openpyxl')
        df.to_csv(output_file, sep='|', index=False)
        print(f"Conversion successful. CSV file saved at: {output_file}")
    except Exception as e:
        print(f"Error occurred: {e}")

def upload_file(bucket, localpath, s3path):
    client.upload_file(localpath, bucket, s3path)

def download_file(bucket,s3path,downloadpath):
    print(bucket+s3path+downloadpath)
    client.download_file(bucket,s3path,downloadpath)

def movefile(s3_bucket,source,target):
    print(s3_bucket+source+target)
    client.copy_object(Bucket=s3_bucket, CopySource=str('/'+s3_bucket+'/'+source), Key=target)
    client.delete_object(Bucket=s3_bucket, Key=source)