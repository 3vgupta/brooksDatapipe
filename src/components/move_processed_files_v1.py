#Lambda code to move files from extracted folder to processed folder
import json
import os
from datetime import datetime
import boto3


s3_bucket_m=os.environ['bucket']
mapping_json_path=os.environ['mapping_file_path']
client = boto3.client('s3')
# dynamodb_client = boto3.client("dynamodb")
# table_name = 'dev_ucm_datapipe_event_log'
replace_text='processed_folders/'

def lambda_handler(event, context):
    ####

    download_file(s3_bucket_m,mapping_json_path,'/tmp/mapping.json')

#### Reading SQS Que
    if len(event["Records"]) >= 1:
        print('Multiple SQS events,'+str(len(event["Records"])))
        for i in range(0, len(event["Records"])):
            s3eventdata = json.loads(event["Records"][i]["body"])
            print(s3eventdata)
            if len(s3eventdata["Records"]) >= 1:
                print('Multiple S3 events,'+str(len(s3eventdata["Records"])))
            for j in range(0, len(s3eventdata["Records"])):
                s3event = s3eventdata["Records"][j]
                s3_bucket = s3event['s3']['bucket']['name']
                sourceObjectKey = s3event["s3"]["object"]["key"]
                fn=sourceObjectKey.replace(replace_text,'')
                # print(fn)
                str2=fn.replace('.csv','')
                x=str2.split('_')
                tablename=str2[0:-7]
                yearmonth=str2[-6:]
                f=open('/tmp/mapping.json')
                jsond=json.load(f)
                table=tablename
                level1=''
                for i in jsond:
                    if jsond[i]['table']==table:
                        level1=jsond[i]['level1']
                        break
                response = client.list_objects_v2(Bucket= s3_bucket, Prefix = 'raw/bpe/'+yearmonth+'/'+level1+'/'+tablename+'/extracted_files')
                # print(response)
                print(len(response["Contents"]))
                if len(response["Contents"])==0:
                    print('No files in the '+ 'raw/bpe/'+yearmonth+'/'+level1+'/'+tablename+'/extracted_files'+' folder')
                    return 1
                for i in range(0,len(response["Contents"])):
                    if response["Contents"][i]["Key"][-3:]=='csv':
                        #print(response["Contents"][i]["Key"])
                        source_location=response["Contents"][i]["Key"]
                        target_location=source_location.replace('extracted_files','processed_files')
                        movefile(s3_bucket,source_location,target_location)
                print('files moved from extracted to processed folder')
                client.delete_object(Bucket=s3_bucket, Key=sourceObjectKey)
                os.remove('/tmp/mapping.json')


def movefile(s3_bucket,source,target):
    print(s3_bucket+source+target)
    client.copy_object(Bucket=s3_bucket, CopySource=str('/'+s3_bucket+'/'+source), Key=target)
    client.delete_object(Bucket=s3_bucket, Key=source)


def download_file(bucket,s3path,downloadpath):
    print(bucket+s3path+downloadpath)
    client.download_file(bucket,s3path,downloadpath)