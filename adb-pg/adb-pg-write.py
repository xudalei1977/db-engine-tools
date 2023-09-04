# -*- coding: utf-8 -*-
import sys, datetime, time, logging, threading, random
import pymysql

gWritenRecord = 0
gStartTime = 0
gEndTime = 0

concurrent = int(sys.argv[1])
recordsPerThread = int(sys.argv[2])
mylock = threading.RLock()

class writeThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

        self.conn = pymysql.connect(host='am-bp1450e89xc2004xj167320.ads.aliyuncs.com',
                                port=3306, user='admin', passwd='******', db='dev')

        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        global gWritenRecord

        self.write_records()

        mylock.acquire()
        gEndTime = time.time()
        gWritenRecord += self.recordsPerThread
        print("%s done, %s seconds past, %d reocrds saved" % (self.getName(), gEndTime - gStartTime, gWritenRecord))
        mylock.release()

        self.cursor.close()
        self.conn.close()

    def write_records(self):
        for i in range(0, self.recordsPerThread):
            sql = 'insert into web_sales values (2450816,39032,2450878,%s,7035469,1534466,5463,3831213,10617470,1104006,5357,3386754,1664,2,3,15,240,%s,85,16.77,35.72,16.78,1609.9,1426.3,1425.45,3036.2,14.26,0,1487.5,1426.3,1440.56,2913.8,2928.06,-20000)'
            self.cursor.execute(sql, (random.randrange(300000), random.randrange(60000000)))

if __name__ == "__main__":
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = writeThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will write %d records" % (concurrent, recordsPerThread))
