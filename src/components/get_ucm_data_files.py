#Lambda code to extract bicc extracts from UCM
import requests
from requests import Session
from zeep import Client
from zeep.transports import Transport
import zipfile36 as zipfile
import json
from requests.auth import HTTPBasicAuth
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
import logging
import boto3
import sys

urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

s3_bucket=os.environ['bucket']
user=os.environ['user']
url=os.environ['url']
password=os.environ['pwd']
mapping_json_path=os.environ['mapping_file_path']
client = boto3.client('s3')
dynamodb_client = boto3.client("dynamodb")
table_name = 'dev_ucm_datapipe_event_log'

def lambda_handler(event, context):
    
    ####
    try:
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
                    s3_bucket_q = s3event['s3']['bucket']['name']
                    sourceObjectKey = s3event["s3"]["object"]["key"]
                    print(s3_bucket)
                    print(sourceObjectKey)
                    file_name=sourceObjectKey.replace('datafilesjson/','')
                    file_id=int(file_name[:6])
                    print(file_id)
#### Dwonloading UCM File 
        put_item_in_database(str(file_id),str(datetime.now()),'File download started','N/A','N/A')
        file_name=download_ucm_files(url,str(file_id),'/tmp/')
        put_item_in_database(str(file_id),str(datetime.now()),'File download Completed',file_name,'N/A')
        print(file_name)

#### Downloading Mapping Json
        download_file(s3_bucket,mapping_json_path+'mapping.json','/tmp/mapping.json')

#### Creating value for year month for S3 folder
        current_month = datetime.now().strftime('%m')
        current_year = datetime.now().strftime('%Y')
        datefolder=str(current_year)+str(current_month)
        f = open('/tmp/mapping.json')
        data = json.load(f)
#### Extracting CSV from Zip File
        key=str(file_id)+'-'+file_name
        with zipfile.ZipFile('/tmp/'+key,"r") as f:
            f.extractall('/tmp/')
        s3_folder=s3_folder_key_csv(file_name)
        s3_folder=s3_folder.replace('file_','')
        s3_folder=s3_folder.replace('_','.')
        print(s3_folder)
        try:
            s3_path=data[s3_folder]['parent']+datefolder+'/'+data[s3_folder]['level1']+'/'+data[s3_folder]['table']+'/zip_file/'+str(file_id)+'-'+file_name
        except:
            s3_path='raw/bpe/'+datefolder+'/unmapped/unmapped/'+'zip_file/'+str(file_id)+'-'+file_name
        put_item_in_database(str(file_id),str(datetime.now()),'Zip File Upload to S3 started',file_name,s3_path)
        upload_file(s3_bucket,'/tmp/'+str(file_id)+'-'+file_name,s3_path)
        put_item_in_database(str(file_id),str(datetime.now()),'Zip File Upload to S3 Completed',file_name,s3_path)
        print(key+' is uploaded')
#### Uploading CSV File   
        s3_folder=s3_folder_key_csv(file_name)
        s3_folder=s3_folder.replace('file_','')
        s3_folder=s3_folder.replace('_','.')
        try:
            s3_path=data[s3_folder]['parent']+datefolder+'/'+data[s3_folder]['level1']+'/'+data[s3_folder]['table']+'/extracted_files/'+str(file_id)+'-'+(file_name.replace('.zip','.csv'))
        except:
            s3_path='raw/bpe/'+datefolder+'/unmapped/unmapped/'+'extracted_files/'+str(file_id)+'-'+(file_name.replace('.zip','.csv'))
        put_item_in_database(str(file_id),str(datetime.now()),'CSV File Upload to S3 started',(file_name.replace('.zip','.csv')),s3_path)
        upload_file(s3_bucket,'/tmp/'+(file_name.replace('.zip','.csv')),s3_path)
        put_item_in_database(str(file_id),str(datetime.now()),'CSV File Upload to S3 Completed',(file_name.replace('.zip','.csv')),s3_path)
        print(file_name.replace('.zip','.csv')+' is uploaded')
        os.remove('/tmp/'+(file_name.replace('.zip','.csv')))
        os.remove('/tmp/'+str(file_id)+'-'+file_name)
        os.remove('/tmp/'+'mapping.json')
        movefile(s3_bucket_q,str('datafilesjson/'+str(file_id)+'.json'),str('processed/'+str(file_id)+'.json'))
    except:
      print("Error: During execution of get ucm data files")
      error = sys.exc_info()
      print(error)
      send_email_notification('Error: During execution of get ucm data files - '+str(file_id), error)
      
    

def download_ucm_files(url,file_id,path):
   docid=file_id
   session = Session()
   session.auth = HTTPBasicAuth(user,password)
   client = Client(url+'?WSDL',transport=Transport(session=session))
   # Create Service Request
   ser_request = client.get_type('ns0:Service')
   ser = ser_request(Document= { 'Field': { 'name':'dID','_value_1':docid} } , IdcService= 'GET_FILE')
   # Make request
   res = client.service.GenericSoapOperation(Service=ser,webKey='cs')
   # The response object will have the requested file as an attachment. Lets process this and save the file
   fileName = path+docid+'-'+res['Service']['Document']['File'][0].href
   with open(fileName , 'wb') as f:
      f.write(res['Service']['Document']['File'][0].Contents)
   return  res['Service']['Document']['File'][0].href

def extract_files(key):
    #print('extracting-'+key)
    with zipfile.ZipFile('datafiles/'+key,"r") as f:
        f.extractall('/tmp/extracted_files/')

def upload_file(bucket, localpath, s3path):
    client.upload_file(localpath, bucket, s3path)

def download_file(bucket,s3path,downloadpath):
    print(bucket+s3path+downloadpath)
    client.download_file(bucket,s3path,downloadpath)

def s3_folder_key(name):
    pend=name.find('-batch')
    pstart=(name[:pend]).find('-')
    fstr=name[pstart+1:pend]
    return fstr

def s3_folder_key_csv(name):
    pend=name.find('-batch')
    pstart=0
    fstr=name[pstart:pend]
    return fstr

def movefile(s3_bucket,source,target):
    print(s3_bucket+source+target)
    client.copy_object(Bucket=s3_bucket, CopySource=str('/'+s3_bucket+'/'+source), Key=target)
    client.delete_object(Bucket=s3_bucket, Key=source)
# def put_item_in_database(file,timestmp,listvalue):
#   print('Loading data to dynamodb')

#   response = dynamodb_client.put_item(
#   TableName=table_name,
#   Item={
#         "manifest_file_id": {"S": file},
#         "timestamp": {"S": timestmp},
#         "subject": {"S": listvalue},
#     },)
    
def put_item_in_database(fileid,timestmp,event,fn,s3p):
   print('Loading data to dynamodb')
   response = dynamodb_client.put_item(
   TableName=table_name,
   Item={
        "file_id": {"S": fileid},
        "time_stamp": {"S": timestmp},
        "event": {"S": event},
        "filename":{"S":fn},
        "s3path":{"S":s3p},
    },)
    


def send_email_notification(step, error):
    SENDER = os.environ['SENDER']
    RECIPIENT = os.environ['RECIPIENT']
    AWS_REGION = os.environ['REGION']
    SUBJECT = "Failure:Download UCM Data Files"
    BODY_TEXT = (str("File process failed for\r\n"+step)
                 )

# The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Data extraction failed - {step}</h1>
      <p>This email was sent to notify there has been error while executing get UCM Data Files,
        Please check error log for detailed error information,
        {error}
      </p>
    </body>
    </html>
                """.format(step=step, error=error)

# The character encoding for the email.
    CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
    client_ses = boto3.client('ses', region_name=AWS_REGION)

# Try to send the email.
    try:
        # Provide the contents of the email.
        response = client_ses.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])