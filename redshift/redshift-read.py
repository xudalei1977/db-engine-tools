# -*- coding: utf-8 -*-
import sys, os, time, logging, datetime
import concurrent.futures
import redshift_connector

class Task:
    def __init__(self, name, recordsPerThread):
        self.name = name
        self.recordsPerThread = recordsPerThread
        self.conn = redshift_connector.connect(host = 'htap-demo-wg.551831295244.us-east-1.redshift-serverless.amazonaws.com',
                                               database = 'taxi_trips',
                                               port = 5439,
                                               user = 'admin',
                                               password = '*****')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.cursor.execute('SET enable_result_cache_for_session TO false')

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
        tpep_pickup_datetime_begin = '2023-01-01 00:00:00'
        tpep_pickup_datetime_end = '2023-01-01 06:30:00'
        for i in range(0, self.recordsPerThread):
            sql_detail = '''select sum("fare_amount") from "taxi_trips"."nyc_source"
                     where "VendorID" = %s
                     and "tpep_pickup_datetime" >= to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS') and "tpep_pickup_datetime" < to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS')
                     and "PULocationID" = %s and "DOLocationID" = %s and "payment_type" = %s'''
            #self.cursor.execute(sql_detail, (param_array[i%10][0], tpep_pickup_datetime_begin, tpep_pickup_datetime_end, param_array[i%10][1], param_array[i%10][2], param_array[i%10][3]))
            self.cursor.execute(sql_detail, (random.randrange(1, 3), tpep_pickup_datetime_begin, tpep_pickup_datetime_end, random.randrange(1, 266), random.randrange(1, 266), random.randrange(1, 7)))
            row_detail = self.cursor.fetchone()
        return (0 if row_detail[0] is None else row_detail[0])

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
