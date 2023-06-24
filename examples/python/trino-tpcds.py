import csv
import json
import time
import trino
from trino import transaction
import sys
import getopt
import os
import time
from datetime import datetime
from urllib.parse import urlparse

SQLFILES = ''
HOST = ''
PORT = ''
CATALOG = 'tpcds'
OUTPUT = './'
SCHEMA = 'default'

'''
Trino TPC DS 测试

'''
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "f:h:p:c:o:s:",
                               ["sqlfiles=",
                                "host=",
                                "port=",
                                "catalog=",
                                "output="])
    for opt_name, opt_value in opts:
        if opt_name in ('-f', '--sqlfiles'):
            SQLFILES = opt_value
            print("SQLFILES:" + SQLFILES)
        elif opt_name in ('-h', '--host'):
            HOST = opt_value
            print("HOST:" + HOST)
        elif opt_name in ('-p', '--port'):
            PORT = opt_value
            print("PORT:" + PORT)
        elif opt_name in ('-c', '--catalog'):
            CATALOG = opt_value
            print("CATALOG:" + CATALOG)
        elif opt_name in ('-s', '--schema'):
            SCHEMA = opt_value
            print("SCHEMA:" + SCHEMA)
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
/Users/xiohuang/IdeaProjects/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_athena
'''

#write result csv
writeresultfile = "{:s}/result_{:s}_{:s}.csv".format(OUTPUT, CATALOG, SCHEMA)

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


    conn = trino.dbapi.connect(
        host=HOST,
        port=PORT,
        user='trino',
        catalog=CATALOG,
        schema=SCHEMA,
        isolation_level=transaction.IsolationLevel.READ_COMMITTED
    )

    starttime = int(round(time.time()*1000))

    cursor = conn.cursor()
    cursor.execute(sqltext)
    rows = cursor.fetchall()
    # print(rows)
    cursor.close()
    conn.close()

    endtime = int(round(time.time()*1000))

    # print(json.dumps(query_results['QueryRuntimeStatistics']['Timeline'], indent=10, sort_keys=False))
    # print(int(query_results['QueryRuntimeStatistics']['Timeline']['QueryQueueTimeInMillis']))

    with open(writeresultfile, "a+", newline='') as csvfile:
        fieldnames = ['SQL', 'ExecuteTime', 'Rows']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #
        writer.writerow({'SQL': filename,
                         'ExecuteTime': int(endtime - starttime),
                         'Rows': len(rows)})


load_sql_file(SQLFILES)
