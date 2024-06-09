# -*- coding: utf-8 -*-
import sys, os, time, logging, datetime
import concurrent.futures
import redshift_connector


# MASTER_IP="<Master-IP>"
# TABLE_NAME="usertable"
#
# cat << EOF | sudo -u hbase hbase shell >> add-peer.out &
# add_peer 'PEER_$TABLE_NAME', CLUSTER_KEY => '$MASTER_IP:2181:/hbase'
# disable_peer 'PEER_$TABLE_NAME'
# set_peer_tableCFs 'PEER_$TABLE_NAME', '$TABLE_NAME'
# enable_table_replication '$TABLE_NAME'
# EOF


# function: build the hbase replication, if user provide the table, then build the replication ONLY for it, otherwise, build replication for

def main():
    if len(sys.argv) == 3:
        parallel = int(sys.argv[1])
        recordsPerThread = int(sys.argv[2])
    else:
        print("Job failed. Please provided params")
        sys.exit(1)


    # get all tables in the source cluster




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
