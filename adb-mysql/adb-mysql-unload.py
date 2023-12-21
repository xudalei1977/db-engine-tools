# -*- coding: utf-8 -*-
import sys, time, logging, threading, random, os
import psycopg2
from datetime import datetime
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

gStartTime = 0
gEndTime = 0
gHost = "am-bp1z0ir9k33fkr21590650.ads.aliyuncs.com"
gOss = "oss://dalei-demo/cdn"
gDir = "/home/ec2-user"

mylock = threading.RLock()

class adbUnloadThread(threading.Thread):
    def __init__(self, threadId, tableName, directory):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.threadId = threadId
        self.tableName = tableName
        self.directory = directory

        self.conn = pymysql.connect(host='am-bp1z0ir9k33fkr21590650.ads.aliyuncs.com',
                                        port=3306, user='admin', passwd='******', db='dev')

                self.conn.autocommit = True
                self.cursor = self.conn.cursor()

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        self.unload_table()

        mylock.acquire()
        gEndTime = time.time()
        print("%s done, %s seconds past, %s table unload completed." % (self.getName(), gEndTime - gStartTime, self.tableName))
        mylock.release()

    def unload_table(self):
        #unload_cmd = "psql -h %s -p 5432 -U postgres -d dev -q -c \"\\COPY %s TO '%s/%s.csv'\"" % (gHost, self.tableName, self.directory, self.tableName)
        #os.system(pass_cmd + unload_cmd)
        cursor.execute("SELECT * FROM %s" % self.tableName)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, self.directory + "/" + self.tableName + ".parquet")

        # zip and upload to oss
        os.system("gzip %s/%s.parquet" % (self.directory, self.tableName))
        os.system("ossutil cp %s/%s" % (gOss, dir))

        # send message to SQS


if __name__ == "__main__":

    gStartTime = time.time()
    threadId = 0

    # create the directory in local and oss
    curr_date = datetime.now().strftime("%Y-%m-%d")
    curr_dir = "adb_unload_%s" % curr_date
    os.mkdir(gDir + "/" + curr_dir)
    os.system("ossutil mkdir %s/%s" % (gOss, curr_dir)

    # iterate all tables
    try:
        conn = pymysql.connect(host=gHost,
                               port=3306, user='admin', passwd='******', db='dev')
        sql = "show tables"
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()
        threadId += 1
        for row in records:
            print("the table is := %s" % row[0])
            t = adbUnloadThread(threadId, row[0], gDir + "/" + curr_dir)
            t.start()
        cur.close()
    except (Exception, DatabaseError) as e:
        print("connect adm-mysql failed: ", e)
    finally:
        if connection is not None:]
            connection.close()
            print("adb-mysql is closed.")


