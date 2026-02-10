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
DATABASE = 'dev'
SCHEMA = 'iceberg'
USERNAME = 'awsuser'
PASSWORD = 'awsuser'
CLUSTER_IDENTIFIER = 'examplecluster'
USE_IAM = False  # Set to True for S3 Tables with Federated user authentication


###
### Redshift TPC-DS Test
### Traditional authentication:
### python redshift-tpcds.py -f /home/ec2-user/environment/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift \
###    -h <redshift-endpoint> -o /home/ec2-user/environment/redshift -u awsuser -p password
###
### IAM/Federated authentication (for S3 Tables):
### python redshift-tpcds.py -f /home/ec2-user/environment/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift \
###    -h <redshift-endpoint> -o /home/ec2-user/environment/redshift -c <cluster-identifier> --iam

if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "f:h:o:u:p:s:d:c:",
                               ["sqlfiles=",
                                "host=",
                                "output=",
                                "username=",
                                "password=",
                                "schema=",
                                "database=",
                                "cluster_identifier=",
                                "iam"])
    for opt_name, opt_value in opts:
        if opt_name in ('-f', '--sqlfiles'):
            SQLFILES = opt_value
            print("SQLFILES:" + SQLFILES)
        elif opt_name in ('-h', '--host'):
            HOST = opt_value
            print("HOST:" + HOST)
        elif opt_name in ('-c', '--cluster_identifier'):
            CLUSTER_IDENTIFIER = opt_value
            print("CLUSTER_IDENTIFIER:" + CLUSTER_IDENTIFIER)
        elif opt_name in ('-u', '--username'):
            USERNAME = opt_value
            print("USERNAME:" + USERNAME)
        elif opt_name in ('-p', '--password'):
            PASSWORD = opt_value
        elif opt_name in ('-o', '--output'):
            OUTPUT = opt_value
            print("OUTPUT:" + OUTPUT)
        elif opt_name in ('-d', '--database'):
            DATABASE = opt_value
            print("DATABASE:" + DATABASE)
        elif opt_name in ('-s', '--schema'):
            SCHEMA = opt_value
            print("SCHEMA:" + SCHEMA)
        elif opt_name == '--iam':
            USE_IAM = True
            print("USE_IAM: True (Federated user authentication)")
        else:
            print("need parameters [sqlfiles,region,database etc.]")
            exit()

else:
    print("Job failed. Please provided params sqlfiles,region .etc ")
    sys.exit(1)

##
##Load SQL files from
##/Users/xiohuang/IdeaProjects/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift


#write result csv
writeresultfile = "{:s}/result_{:s}.csv".format(OUTPUT, SCHEMA)

def load_sql_file(sqlpath):
    ## write header of result file
    with open(writeresultfile, "w", newline='') as csvfile:
        fieldnames = ['SQL', 'ExecuteTime', 'Rows']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    i = 0


    for root, dirs, files in os.walk(sqlpath):
        for file in sorted(files):
            if os.path.splitext(file)[1] == '.sql':
                sqlfilepath = os.path.join(root, file)
                sqlfile = open(sqlfilepath, encoding='utf-8')
                sqltext = sqlfile.read()
                sqlfile.close()

                print('exec sql:' + sqlfilepath)
                executeSQL(file, sqltext)
                i = i + 1
                print("process:[{:d}/105]".format(i))


def executeSQL(filename, sqltext):
    
    # Connect using IAM authentication (for S3 Tables / Federated user)
    if USE_IAM:
        conn = redshift_connector.connect(
            host=HOST,
            database=DATABASE,
            port=5439,
            iam=True,
            cluster_identifier=CLUSTER_IDENTIFIER,
            timeout=600
        )
    # Connect using traditional username/password authentication
    else:
        conn = redshift_connector.connect(
            host=HOST,
            database=DATABASE,
            port=5439,
            user=USERNAME,
            password=PASSWORD,
            timeout=600
        )
    rowcount = 0
    starttime = int(round(time.time()*1000))
    cursor = conn.cursor()
    try:
        # print(sqltext.format(SCHEMA))
        quoted_db = f'"{DATABASE}"' if any(c in DATABASE for c in ['-', '@', ' ']) else DATABASE
        quoted_schema = f'"{SCHEMA}"' if any(c in SCHEMA for c in ['-', '@', ' ']) else SCHEMA
        cursor.execute(sqltext.format(quoted_db, quoted_schema))
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
                         'ExecuteTime': float(endtime - starttime)/1000,
                         'Rows': rowcount})


load_sql_file(SQLFILES)
