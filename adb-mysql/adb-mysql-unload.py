# -*- coding: utf-8 -*-
import sys, time, logging, threading, random, os
import psycopg2
from datetime import datetime
import boto3

gStartTime = 0
gEndTime = 0
gHost = "gp-bp10cn2866q7t90ub-master.gpdb.rds.aliyuncs.com"
gOss = "oss://dalei-demo/cdn"
gDir = "/home/ec2-user"

mylock = threading.RLock()

class adbUnloadThread(threading.Thread):
    def __init__(self, threadId, tableName, directory):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.threadId = threadId
        self.tableName = tableName
        self.directory = directory

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        self.unload_table()

        mylock.acquire()
        gEndTime = time.time()
        print("%s done, %s seconds past, %s table unload completed." % (self.getName(), gEndTime - gStartTime, self.tableName))
        mylock.release()

    def unload_table(self):
        pass_cmd = "export PGPASSWORD=******;"
        unload_cmd = "psql -h %s -p 5432 -U postgres -d dev -q -c \"\\COPY %s TO '%s/%s.csv'\"" % (gHost, self.tableName, self.directory, self.tableName)
        os.system(pass_cmd + unload_cmd)

        # zip and upload to oss
        os.system("gzip %s/%s.csv" % (self.directory, self.tableName))
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
        conn = psycopg2.connect(host=gHost,
                                port=5432, dbname="dev",
                                user="postgres", password="******")
        sql = "select tablename from pg_tables where schemaname = 'public'"
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()
        threadId += 1
        for row in records:
            print("the table is := %s" % row[0])
            t = adbUnloadThread(threadId, row[0], gDir + "/" + curr_dir)
            t.start()
        cur.close()    # 关闭游标
    except (Exception, DatabaseError) as e:
        print("连接 PostgreSQL 失败：", e)
    finally:
        if connection is not None:    # 释放数据库连接
            connection.close()
            print("PostgreSQL 数据库连接已关闭。")


