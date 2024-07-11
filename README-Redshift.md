# Redshift Spectrum Benchmark 


## Redshift for Iceberg
该benchmark测试，用于测试 Redshift 支持 Iceberg 查询的场景。Redshift 通过 spectrum 支持从S3访问Iceberg的数据文件。限制
* 只支持Glue Data Catalog中定义的Iceberg表。
* 查询的表必须已经存在于Glue Data Catalog 中，因此不支持从Redshift创建或者修改外部表的命令
* 不支持 Time travel 查询
* 支持Iceberg 的 V1 V2 表。

在Redshift中创建External Schema，database 关联到 Glue Data Catalog 的数据库名称
```sql
CREATE external schema spectrum_iceberg_schema
from data catalog
database 'tpcds_iceberg'
region 'us-east-1'
iam_role default;
```

测试提交脚本
```shell
cd emr-on-eks-benchmark
export SQLFILE=./spark-sql-perf/src/main/resources/tpcds_2_4_redshift
export REDSHIFT_SERVER=<redshift-endporint>
export OUTPUT_PATH=<log-path>
export USERNAME=<username>
export PASSWORD=<password>
python ./examples/python/redshift-tpcds.py -f $SQLFILE -h $REDSHIFT_SERVER -u $USERNAME -p $PASSWORD -o $OUTPUT_PATH
```

