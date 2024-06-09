# -*- coding: utf-8 -*-
import sys, datetime, time, logging, threading, random
import pymysql

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

print("********** before conn %s " % (str(datetime.datetime.now())))

conn = pymysql.connect(host='htap-demo.cluster-chl9yxs6uftz.us-east-1.rds.amazonaws.com',
                       port=3306, user='admin', passwd='*******', db='taxi_trips_agg')
conn.autocommit = True
cursor = conn.cursor()

print("********** before sql_sum %s " % (str(datetime.datetime.now())))

for i in range(0, 10):
    sql_sum = '''select sum(fare_amount) from nyc_hour_aggregation
             where VendorID = %s and agg_date = str_to_date(%s, '%%Y-%%m-%%d') and agg_hour <= %s and PULocationID = %s and DOLocationID = %s and payment_type = %s'''
    cursor.execute(sql_sum, (param_array[0][0], agg_date, agg_hour, param_array[0][1], param_array[0][2], param_array[0][3]))
    row = cursor.fetchone()
    row_sum = 0 if row[0] is None else row[0]

    print("********** before sql_detail %s " % (str(datetime.datetime.now())))

    sql_detail = '''select sum(fare_amount) from taxi_trips.nyc_source
             where VendorID = %s
             and tpep_pickup_datetime >= str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s') and tpep_pickup_datetime < str_to_date(%s, '%%Y-%%m-%%d %%H:%%i:%%s')
             and PULocationID = %s and DOLocationID = %s and payment_type = %s'''
    cursor.execute(sql_detail, (param_array[0][0], tpep_pickup_datetime_begin, tpep_pickup_datetime_end, param_array[0][1], param_array[0][2], param_array[0][3]))
    row = cursor.fetchone()
    row_detail = 0 if row[0] is None else row[0]
    print(f'*********** result := {row_sum + row_detail}')

print("********** end sql_detail %s " % (str(datetime.datetime.now())))
