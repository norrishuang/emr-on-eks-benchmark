import csv
import json
import time
import boto3
import sys
import getopt
import os
from urllib.parse import urlparse

SQLFILES = ''
REGION = ''
DATABASE = 'tpcds'
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                           "f:r:d:",
                           ["sqlfiles=",
                            "region=",
                            "database="])
    for opt_name, opt_value in opts:
        if opt_name in ('-f', '--sqlfiles'):
            SQLFILES = opt_value
            print("SQLFILES:" + SQLFILES)
        elif opt_name in ('-r', '--region'):
            REGION = opt_value
            print("REGION:" + REGION)
        elif opt_name in ('-d', '--database'):
            DATABASE = opt_value
            print("DATABASE:" + DATABASE)
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
def load_sql_file(sqlpath):

    ##写结果集的表头
    with open("./result_" + DATABASE + ".csv", "a+", newline='') as csvfile:
        fieldnames = ['SQL', 'QueryQueueTimeInMillis', 'EngineExecutionTimeInMillis', 'ServiceProcessingTimeInMillis', 'TotalExecutionTimeInMillis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    for root, dirs, files in os.walk(sqlpath):
        for file in files:
            sqlfilepath = os.path.join(root, file)
            # print(sqlfilepath)
            sqlfile=open(sqlfilepath, encoding='utf-8')
            sqltext=sqlfile.read()
            sqlfile.close()
            # print(sqltext)
            # if("q1.sql" in sqlfilepath):
            #     print('exec sql:' + sqlfilepath)
            #     print(sqltext)
            #     executeSQL(file, sqltext)
            print('exec sql:' + sqlfilepath)
            executeSQL(file, sqltext)


def executeSQL(filename, sqltext):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=sqltext,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': 's3://aws-athena-query-results-us-east-1-812046859005/',
        },
        WorkGroup='primary'
    )

    print(response)
    while True:
        try:
            query_results = client.get_query_runtime_statistics(
                QueryExecutionId=response['QueryExecutionId']
            )
            break
        except Exception as err:
            responseerr = err.response
            code = responseerr['Error']['Code']
            message = responseerr['Error']['Message']

            if 'Query has not yet finished' in message:
                time.sleep(5)
            else:
                raise(err)

    print(json.dumps(query_results['QueryRuntimeStatistics']['Timeline'], indent=10, sort_keys=False))
    # print(int(query_results['QueryRuntimeStatistics']['Timeline']['QueryQueueTimeInMillis']))

    with open("./result_" + DATABASE + ".csv", "a+", newline='') as csvfile:
        fieldnames = ['SQL', 'QueryQueueTimeInMillis', 'EngineExecutionTimeInMillis', 'ServiceProcessingTimeInMillis', 'TotalExecutionTimeInMillis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #
        # writer.writeheader()
        writer.writerow({'SQL': filename,
                         'QueryQueueTimeInMillis': int(query_results['QueryRuntimeStatistics']['Timeline']['QueryQueueTimeInMillis']),
                         'EngineExecutionTimeInMillis': int(query_results['QueryRuntimeStatistics']['Timeline']['EngineExecutionTimeInMillis']),
                         'ServiceProcessingTimeInMillis': int(query_results['QueryRuntimeStatistics']['Timeline']['ServiceProcessingTimeInMillis']),
                         'TotalExecutionTimeInMillis': int(query_results['QueryRuntimeStatistics']['Timeline']['TotalExecutionTimeInMillis'])})


load_sql_file(SQLFILES)

'''
{
	"QueryExecutionId": "b774ac88-c115-4d49-acc1-310f5822288a",
	'ResponseMetadata': {
		'RequestId': '83271a2f-bcc2-4174-96d4-3d0330bfe338',
		'HTTPStatusCode': 200,
		'HTTPHeaders': {
			'date': 'Fri, 23 Jun 2023 02:48:40 GMT',
			'content-type': 'application/x-amz-json-1.1',
			'content-length': '59',
			'connection': 'keep-alive',
			'x-amzn-requestid': '83271a2f-bcc2-4174-96d4-3d0330bfe338'
		},
		'RetryAttempts': 0
	}
}'''
