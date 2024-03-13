import json
import datetime
import pandas as pd
import boto3
import os

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = os.environ.get('AWS_SNS_ARN')

def lambda_handler(event, context):
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        
        df_s3_data = pd.read_json(resp['Body'])
        df_delivered = df_s3_data[df_s3_data.status == 'delivered']
        
        json_string = df_delivered.to_json(orient='records')
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        file_name = f"{current_date}-processed.json"

        tgt_bucket_name = 'aws-de-doordash-target-zn'
        file_path = 'processed_data/'+file_name
        s3_client.put_object(Body=json_string, Bucket=tgt_bucket_name, Key=file_path)

        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+tgt_bucket_name+"/"+file_path)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    

    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')


