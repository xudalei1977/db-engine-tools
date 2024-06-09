# -*- coding: utf-8 -*-
import logging
import json
import boto3

def handler(event, context):
  # Load event content.
  oss_raw_data = json.loads(event)
  # Get oss event related parameters passed by oss trigger.
  oss_info_map = oss_raw_data['events'][0]['oss']
  bucket_name = oss_info_map['bucket']['name']
  object_name = oss_info_map['object']['key']

  client = boto3.client('sqs',
                        aws_access_key_id='AK********',
                        aws_secret_access_key='Sz********',
                        region_name='<Your-AWS-Region>')
  # Send message to AWS SQS
  try:
      message = client.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/<Your-AWS-Account-ID>/oss-2-s3',
                                MessageBody=('oss://' + bucket_name + '/' + object_name))
  except Exception as err:
      print("An error occurred")
      raise err
  return message