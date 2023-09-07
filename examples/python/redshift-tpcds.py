import csv
import json
import time
import redshift_connector
import sys
import getopt
import os
import time
from datetime import datetime
from urllib.parse import urlparse

SQLFILES = ''
HOST = ''
OUTPUT = './'
SCHEMA = 'iceberg'

'''
Trino TPC DS 测试

'''
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "f:h:p:c:o:s:",
                               ["sqlfiles=",
                                "host=",
                                "output="])
    for opt_name, opt_value in opts:
        if opt_name in ('-f', '--sqlfiles'):
            SQLFILES = opt_value
            print("SQLFILES:" + SQLFILES)
        elif opt_name in ('-h', '--host'):
            HOST = opt_value
            print("HOST:" + HOST)
        elif opt_name in ('-o', '--output'):
            OUTPUT = opt_value
            print("OUTPUT:" + OUTPUT)
        else:
            print("need parameters [sqlfiles,region,database etc.]")
            exit()

else:
    print("Job failed. Please provided params sqlfiles,region .etc ")
    sys.exit(1)

'''
Load SQL files from
/Users/xiohuang/IdeaProjects/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift
'''

#write result csv
writeresultfile = "{:s}/result_{:s}.csv".format(OUTPUT, SCHEMA)

def load_sql_file(sqlpath):
    ##写结果集的表头 覆盖原来的文件
    with open(writeresultfile, "w", newline='') as csvfile:
        fieldnames = ['SQL', 'ExecuteTime', 'Rows']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    i = 0
    for root, dirs, files in os.walk(sqlpath):
        for file in sorted(files):
            sqlfilepath = os.path.join(root, file)
            sqlfile = open(sqlfilepath, encoding='utf-8')
            sqltext = sqlfile.read()
            sqlfile.close()

            print('exec sql:' + sqlfilepath)
            executeSQL(file, sqltext)
            i = i + 1
            print("process:[{:d}/105]".format(i))


def executeSQL(filename, sqltext):

    conn = redshift_connector.connect(
        host=HOST,
        database='dev',
        port=5439,
        user='awsuser',
        password='Amazon123!'
    )
    rowcount = 0
    starttime = int(round(time.time()*1000))
    cursor = conn.cursor()
    try:
        cursor.execute(sqltext)
        rows = cursor.fetchall()
        rowcount = len(rows)
        cursor.close()
    except Exception as err:
        print(err)
        # raise err
        # print(rows)
        cursor.close()
        # conn.close()
        rowcount = -999
    endtime = int(round(time.time()*1000))
    conn.close()
    # print(json.dumps(query_results['QueryRuntimeStatistics']['Timeline'], indent=10, sort_keys=False))
    # print(int(query_results['QueryRuntimeStatistics']['Timeline']['QueryQueueTimeInMillis']))

    with open(writeresultfile, "a+", newline='') as csvfile:
        fieldnames = ['SQL', 'ExecuteTime', 'Rows']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #
        writer.writerow({'SQL': filename,
                         'ExecuteTime': int(endtime - starttime),
                         'Rows': rowcount})


load_sql_file(SQLFILES)
