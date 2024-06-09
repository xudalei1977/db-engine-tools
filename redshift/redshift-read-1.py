# -*- coding: utf-8 -*-
import sys, datetime, time, logging, threading, random
import redshift_connector

gReadRecord = 0
gStartTime = 0
gEndTime = 0

concurrent = int(sys.argv[1])
recordsPerThread = int(sys.argv[2])

param_array = [(2, 161, 142, 1),
               (1, 68,  249, 1),
               (2, 161, 233, 2),
               (2, 107, 236, 1),
               (2, 255, 234, 1),
               (2, 237, 236, 1),
               (2, 41,  238, 0),
               (2, 121, 197, 2),
               (1, 163, 262, 1),
               (2, 142, 151, 2) ]
agg_date = '2023-01-01'
agg_hour = 5
tpep_pickup_datetime_begin = '2023-01-01 00:00:00'
tpep_pickup_datetime_end = '2023-01-01 06:30:00'

mylock = threading.RLock()

class readThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

        self.conn = redshift_connector.connect(host = 'htap-demo-wg.551831295244.us-east-1.redshift-serverless.amazonaws.com',
                                               database = 'taxi_trips',
                                               port = 5439,
                                               user = 'admin',
                                               password = '*****')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.cursor.execute('SET enable_result_cache_for_session TO false')

    def run(self):
        print("********** %s start at := %s" % (self.getName(), str(datetime.datetime.now())))
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
            sql_detail = '''select sum("fare_amount") from "taxi_trips"."nyc_source"
                     where "VendorID" = %s
                     and "tpep_pickup_datetime" >= to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS') and "tpep_pickup_datetime" < to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS')
                     and "PULocationID" = %s and "DOLocationID" = %s and "payment_type" = %s'''
            self.cursor.execute(sql_detail, (param_array[i%10][0], tpep_pickup_datetime_begin, tpep_pickup_datetime_end, param_array[i%10][1], param_array[i%10][2], param_array[i%10][3]))
            row_detail = self.cursor.fetchone()
            print(f'*********** result := {row_detail[0]}')

if __name__ == "__main__":
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = readThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will read %d records" % (concurrent, recordsPerThread))
