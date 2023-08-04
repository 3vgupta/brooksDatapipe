#Lambda code to pull bip files from UCM
import requests
from requests import Session
from zeep import Client
from zeep.transports import Transport
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
                    file_name=sourceObjectKey.replace('bip_report_id_json/','')
                    file_id=int(file_name[:-5])
                    print(file_id)

#### Dwonloading UCM File 
        put_item_in_database(str(file_id),str(datetime.now()),'File download started','N/A','N/A')
        file_name=download_ucm_files(url,str(file_id),'/tmp/')
        put_item_in_database(str(file_id),str(datetime.now()),'File download Completed',file_name,'N/A')
        print(file_name)

#### Downloading Mapping Json
        download_file(s3_bucket,mapping_json_path+'mapping_bip.json','/tmp/mapping_bip.json')

#### Creating value for year month for S3 folder
        current_month = datetime.now().strftime('%m')
        current_year = datetime.now().strftime('%Y')
        datefolder=str(current_year)+str(current_month)
        f = open('/tmp/mapping_bip.json')
        data = json.load(f)

        sl=file_name.find('-B')+1
        s3_folder=file_name[sl:-25]

        try:
            s3_path=data[s3_folder]['parent']+datefolder+'/'+data[s3_folder]['level1']+'/extracted_files/'+file_name
        except:
            s3_path='raw/bip/'+datefolder+'/unmapped/'+'extracted_files/'+file_name
        put_item_in_database(str(file_id),str(datetime.now()),'CSV File Upload to S3 started',file_name,s3_path)
        upload_file(s3_bucket,'/tmp/'+str(file_id)+'-'+file_name,s3_path)
        put_item_in_database(str(file_id),str(datetime.now()),'CSV File Upload to S3 Completed',file_name,s3_path)
        print(file_name.replace('.zip','.csv')+' is uploaded')
    #os.remove('/tmp/'+file_name)
        os.remove('/tmp/'+str(file_id)+'-'+file_name)
        os.remove('/tmp/'+'mapping_bip.json')
        movefile(s3_bucket_q,str('bip_report_id_json/'+str(file_id)+'.json'),str('processed/bip/'+str(file_id)+'.json'))
    except:
      print("Error: During download of bip report"+'-'+file_id)
      error = sys.exc_info()
      send_email_notification("Error: During download of bip report"+'-'+file_id, error)
    

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


def upload_file(bucket, localpath, s3path):
    client.upload_file(localpath, bucket, s3path)

def download_file(bucket,s3path,downloadpath):
    print(bucket+s3path+downloadpath)
    client.download_file(bucket,s3path,downloadpath)


def movefile(s3_bucket,source,target):
    print(s3_bucket+source+target)
    client.copy_object(Bucket=s3_bucket, CopySource=str('/'+s3_bucket+'/'+source), Key=target)
    client.delete_object(Bucket=s3_bucket, Key=source)
    
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
    SUBJECT = "Failure:Downloading BIP Reports"
    BODY_TEXT = (str("File process failed for\r\n"+step)
                 )

# The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Data extraction failed - {step}</h1>
      <p>This email was sent to notify there has been error while executing get bip reports id,
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