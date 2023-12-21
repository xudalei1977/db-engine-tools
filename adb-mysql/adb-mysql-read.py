# -*- coding: utf-8 -*-
import sys, datetime, time, logging, threading, random
import pymysql

gReadRecord = 0
gStartTime = 0
gEndTime = 0

concurrent = int(sys.argv[1])
recordsPerThread = int(sys.argv[2])

mylock = threading.RLock()

class readThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

        self.conn = pymysql.connect(host='am-bp1z0ir9k33fkr21590650.ads.aliyuncs.com',
                                        port=3306, user='admin', passwd='******', db='dev')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        global gReadRecord

        self.read_records()

        mylock.acquire()
        gEndTime = time.time()
        gReadRecord += self.recordsPerThread
        print("%s done, %s seconds past, %d reocrds read" % (self.getName(), gEndTime - gStartTime, gReadRecord))
        mylock.release()

        self.cursor.close()
        self.conn.close()

    def read_records(self):
        for i in range(0, self.recordsPerThread):
            sql = "select * from web_sales where ws_item_sk = %s and ws_order_number = %s"
            self.cursor.execute(sql, (random.randrange(300000), random.randrange(60000000)))
            results = self.cursor.fetchone()

if __name__ == "__main__":
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = readThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will read %d records" % (concurrent, recordsPerThread))
