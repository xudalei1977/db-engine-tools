# -*- coding: utf-8 -*-
import sys, os, time, logging, datetime, random
import concurrent.futures
import pymysql

class Task:
    def __init__(self, name, recordsPerThread):
        self.name = name
        self.recordsPerThread = recordsPerThread
        self.conn = pymysql.connect(host='htap-demo.cluster-chl9yxs6uftz.us-east-1.rds.amazonaws.com',
                                    port=3306, user='admin', passwd='*******', db='taxi_trips_agg')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def _run_sql_with_result(self):
#         param_array = [(2, 161, 142, 1),
#                        (1, 68,  249, 1),
#                        (2, 161, 233, 2),
#                        (2, 107, 236, 1),
#                        (2, 255, 234, 1),
#                        (2, 237, 236, 1),
#                        (2, 41,  238, 0),
#                        (2, 121, 197, 2),
#                        (1, 163, 262, 1),
#                        (2, 142, 151, 2) ]
        VendorID = random.randrange(1, 3)
        agg_date = '2023-01-01'
        PULocationID = random.randrange(1, 266)
        DOLocationID = random.randrange(1, 266)
        payment_type = random.randrange(1, 7)
        agg_hour = 5
        tpep_pickup_datetime_begin = '2023-01-01 06:00:00'
        tpep_pickup_datetime_end = '2023-01-01 06:30:00'
        for i in range(0, self.recordsPerThread):
            sql_sum = '''select sum(fare_amount) from taxi_trips_agg.nyc_hour_aggregation
                     where VendorID = %s and agg_date = str_to_date(%s, '%%Y-%%m-%%d') and PULocationID = %s and DOLocationID = %s and payment_type = %s and agg_hour <= %s '''
            #self.cursor.execute(sql_sum, (param_array[i%10][0], agg_date, param_array[i%10][1], param_array[i%10][2], param_array[i%10][3], agg_hour))
            self.cursor.execute(sql_sum, (VendorID, agg_date, PULocationID, DOLocationID, payment_type, agg_hour))
            row = self.cursor.fetchone()
            row_sum = 0 if row[0] is None else row[0]

            sql_detail = '''select sum(fare_amount) from taxi_trips.nyc_source
                     where tpep_pickup_datetime >= str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s') and tpep_pickup_datetime < str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s')
                     and VendorID = %s and PULocationID = %s and DOLocationID = %s and payment_type = %s'''
            #self.cursor.execute(sql_detail, (param_array[i%10][0], tpep_pickup_datetime_begin, tpep_pickup_datetime_end, param_array[i%10][1], param_array[i%10][2], param_array[i%10][3]))
            self.cursor.execute(sql_detail, (tpep_pickup_datetime_begin, tpep_pickup_datetime_end, VendorID, PULocationID, DOLocationID, payment_type))
            row = self.cursor.fetchone()
            row_detail = 0 if row[0] is None else row[0]

        return row_sum + row_detail

    def run(self, sql=None):
        run_res = {"name": self.name}
        print(f'Starting {self.name} at := {str(datetime.datetime.now())}')
        start_time = time.time()
        try:
            self._run_sql_with_result()
        except Exception as e:
            print(f'{self.name}, execution error {e}')
        finally:
            end_time = time.time()
            duration = end_time - start_time
            run_res["duration"] = duration
            print(f'{self.name} , execution time: {duration} seconds')
            self.cursor.close()
            self.conn.close()

        return run_res

def main():
    if len(sys.argv) == 3:
        parallel = int(sys.argv[1])
        recordsPerThread = int(sys.argv[2])
    else:
        print("Job failed. Please provided params")
        sys.exit(1)

    print(f'Starting main at := {str(datetime.datetime.now())}')
    tasks = []
    for idx in range(0, parallel):
        task = Task(f'job: {idx}', recordsPerThread)
        tasks.append(task)

    with concurrent.futures.ThreadPoolExecutor(max_workers=int(parallel)) as executor:
        futures = []
        task_res_list = []
        for task in tasks:
            future = executor.submit(task.run, None)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result(timeout=500)
                task_res_list.append(res)
            except Exception as exc:
                print(f'generated an exception: {exc}')

    print(f'End main at := {str(datetime.datetime.now())}')

if __name__ == '__main__':
    main()
