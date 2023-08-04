### Lambda code to convert excel to CSV
import csv
import pandas as pd
from openpyxl import load_workbook
import boto3
import os
from datetime import datetime

bucket='sqs-test-bucket-brooks'
download_folder='poc/excel/'
upload_folder='poc/csv/'
archive_folder='poc/archive/'
file_name='CombinedFilesCopy'
client = boto3.client('s3')


def lambda_handler(event, context):
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