# -*- coding: utf-8 -*-
import oss2, os
import boto3
import redshift_connector

oss_endpoint = 'https://oss-cn-hangzhou.aliyuncs.com'
oss_accessid  = '******'
oss_accesskey  = '******'

s3_bucket = 'dalei-demo'
s3_bucket_prefix = 'tmp/'
s3_region = 'us-east-1'

redshift_host = 'noah-demo-wg.551831295244.us-east-1.redshift-serverless.amazonaws.com'
redshift_database = 'dev'
redshift_port = 5439
redshift_user = 'awsuser'
redshift_password = '******'

def lambda_handler(event, context):
    conn = redshift_connector.connect(host = redshift_host,
                                      database = redshift_database,
                                      port = redshift_port,
                                      user = redshift_user,
                                      password = redshift_password)
    conn.autocommit = True
    cursor = conn.cursor()

    for message in event['Records']:
        process_message(message)

    cursor.close()
    conn.close()
    print("done")

def process_message(message):
    try:
        sqs_msg = message['body']

        # Setup auth, required by OSS sdk.
        auth = oss2.Auth(oss_accessid, oss_accesskey)

        ind_1 = sqs_msg.index('/')
        oss_bucket_name = sqs_msg[: ind_1]
        oss_object_name = sqs_msg[ind_1+1 :]

        ind_2 = sqs_msg.rindex('/')
        local_folder = sqs_msg[ind_1+1 : ind_2]
        local_file = './' + oss_object_name

        # Get the database and table name
        tmp_str = local_folder
        ind_3 = tmp_str.rindex('/')
        table_name = tmp_str[ind_3+1 :]

        while table_name.index('=') > 0:
            tmp_str = tmp_str[: ind_3]
            ind_3 = tmp_str.rindex('/')
            table_name = tmp_str[ind_3+1 :]

        tmp_str = tmp_str[: ind_3]
        ind_3 = tmp_str.rindex('/')

        if ind_3 > 0:
            database = tmp_str[ind_3+1 :]
        else:
            database = tmp_str

        # Create local folder if not exists
        os.makedirs(local_folder, exist_ok = True)

        # Download from oss to local
        oss_bucket = oss2.Bucket(auth, oss_endpoint, oss_bucket_name)
        oss_bucket.get_object_to_file(oss_object_name, local_file)

        # Upload local file to s3
        s3_client = boto3.client('s3')
        s3_object_name = s3_bucket_prefix + sqs_msg[ind_1+1 :]
        response = s3_client.upload_file(local_file, s3_bucket, s3_object_name)

        # Copy file to Redshift
        sql = 'copy ' + database + '.public.' + table_name + ' from \'s3://' + s3_bucket + '/' + s3_object_name + '\' iam_role default parquet region \'' + s3_region + '\''
        cursor.execute(sql)

    except Exception as err:
        print("An error occurred")
        raise err
