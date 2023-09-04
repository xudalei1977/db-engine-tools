# -*- coding: utf-8 -*-
import sys, time, logging, threading, random, os
import psycopg2
from datetime import datetime
import boto3
import shutil

gStartTime = 0
gEndTime = 0
# gHost = "gp-bp10cn2866q7t90ub-master.gpdb.rds.aliyuncs.com"
# gDatabase = "adb_sampledata_tpch"
# gSchema = "public"
# gUser = "postgres"
# gPassword = "********"
# gOss = "oss://dalei-demo/cdn"
# gDir = "/home/ecs-user"
# gAccessKey = "*******"
# gSecretKey = "*******"
# gAWSRegion = "us-east-1"
# gAWSSQSUrl = "https://sqs.us-east-1.amazonaws.com/551831295244/oss-2-s3"

gHost = sys.argv[1]
gDatabase = sys.argv[2]
gSchema = sys.argv[3]
gUser = sys.argv[4]
gPassword = sys.argv[5]
gOss = sys.argv[6]
gDir = sys.argv[7]
gAccessKey = sys.argv[8]
gSecretKey = sys.argv[9]
gAWSRegion = sys.argv[10]
gAWSSQSUrl = sys.argv[11]

mylock = threading.RLock()

class adbUnloadThread(threading.Thread):
    def __init__(self, threadId, tableName, local_dir, oss_dir):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.threadId = threadId
        self.tableName = tableName
        self.local_dir = local_dir
        self.oss_dir = oss_dir

    def run(self):
        print("********** %s start at := %s" % (self.getName(), time.time()))
        global gEndTime
        self.unload_table()

        mylock.acquire()
        gEndTime = time.time()
        print("%s done, %s seconds past, %s table unload completed." % (self.getName(), gEndTime - gStartTime, self.tableName))
        mylock.release()

    def unload_table(self):
        pass_cmd = "export PGPASSWORD=%s;" % (gPassword)
        unload_cmd = "psql -h %s -p 5432 -U %s -d %s -q -c \"\\COPY %s TO '%s/%s.csv'\"" % (gHost, gUser, gDatabase, self.tableName, self.local_dir, self.tableName)
        os.system(pass_cmd + unload_cmd)
        print(unload_cmd)

        # zip and upload to oss
        os.system("gzip %s/%s.csv" % (self.local_dir, self.tableName))
        os.system("ossutil cp %s/%s.csv.gz %s/" % (self.local_dir, self.tableName, self.oss_dir))

        # send message to SQS
        client = boto3.client('sqs',
                              aws_access_key_id=gAccessKey,
                              aws_secret_access_key=gSecretKey,
                              region_name=gAWSRegion)

        message = client.send_message(
            QueueUrl=gAWSSQSUrl,
            MessageBody=(self.oss_dir + "/" + self.tableName + ".csv.gz")
        )

if __name__ == "__main__":

    gStartTime = time.time()
    threadId = 0

    # create the directory in local and oss
    curr_date = datetime.now().strftime("%Y-%m-%d")
    curr_dir = "adb_unload_%s" % curr_date
    shutil.rmtree(gDir + "/" + curr_dir)
    os.mkdir(gDir + "/" + curr_dir)
    os.system("ossutil mkdir %s/%s" % (gOss, curr_dir))

    # iterate all tables
    try:
        conn = psycopg2.connect(host=gHost,
                                port=5432, dbname=gDatabase,
                                user=gUser, password=gPassword)
        sql = "select tablename from pg_tables where schemaname = '%s'" % (gSchema)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            threadId += 1
            print("the table is := %s" % row[0])
            t = adbUnloadThread(threadId, row[0], gDir + "/" + curr_dir, gOss + "/" + curr_dir)
            t.start()
        cursor.close()    # 关闭游标
    except (Exception) as e:
        print("连接 PostgreSQL 失败：", e)
    finally:
        if conn is not None:    # 释放数据库连接
            conn.close()
            print("PostgreSQL 数据库连接已关闭。")

