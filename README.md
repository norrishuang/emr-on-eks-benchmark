## Data Lake component TPC-DS Test on AWS

本项目使用AWS EMR Serverless 测试 Data lake 组件（包括Iceberg/Hudi/Deltalake）的TPC-DS benchmark的表现。

## Prerequisite





## EMR Serverless

### 打包测试镜像

使用与 EMR on EKS 相同的程序，用EMR Serverless 对数据湖组件做TPC DS性能测试。以下打包方案，使用了EMR Serverless 6.10版本作为基础镜像。

```
# stay in the project root directory
cd emr-on-eks-benchmark

export AWS_REGION=us-east-1

# get EMR on EKS base image
# us-east-1
export SRC_ECR_URL=755674844232.dkr.ecr.us-east-1.amazonaws.com
# ap-southeast-1
# export SRC_ECR_URL=671219180197.dkr.ecr.us-east-1.amazonaws.com


########## build ############
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URL
aws ecr create-repository --repository-name spark-benchmark --image-scanning-configuration scanOnPush=true

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $SRC_ECR_URL
# Custom an image on top of the EMR Spark
docker build -t $ECR_URL/spark-benchmark:emr6.10 -f docker/benchmark-util/Dockerfile --no-cache --build-arg SPARK_BASE_IMAGE=public.ecr.aws/emr-serverless/spark/emr-6.10.0:latest .

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URL
docker push $ECR_URL/spark-benchmark:emr6.10
```

创建EMR Serverless Application的时候指定ECR，需要在ECR中对应的 repository 给 EMR Serverless 的Application 赋权，让Application有权限获取ECR中的镜像，[参考](https://docs.aws.amazon.com/zh_cn/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html)



### 提交EMR Serverless任务

#### 生成数据

```shell
SPARK_APPLICATION_ID=<emrserverless-applicationid>
JOB_ROLE_ARN=arn:aws:iam::<accountid>:role/<role-for-emrserverless>
S3_BUCKET=<s3bucket>
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
JOBNAME=Datalake-TPC-DS-DataGeneration-3T

#生成的数据量大，因此这里指定了executor disk的大小（100G）,否则执行过程中会出现磁盘空间不足的错误。
aws emr-serverless start-job-run \
	--application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --name $JOBNAME \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "local:///usr/lib/spark/examples/jars/eks-spark-benchmark-assembly-1.0.jar",
          "entryPointArguments":["s3://'${S3_BUCKET}'/benchmark/BLOG_TPCDS-TEST-3T-partitioned","/opt/tpcds-kit/tools","parquet","3000","300","true","true","true"],
      "sparkSubmitParameters": "--class com.amazonaws.eks.tpcds.DataGeneration --conf spark.driver.cores=2 --conf spark.driver.memory=8G  --conf spark.executor.cores=1 --conf spark.executor.memory=4G --conf spark.executor.instances=100 --conf spark.emr-serverless.executor.disk=100G"}
}' \
    --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
        }
    }
}'
```

需要注意，这里生成的数据还是parquet格式，可以通过Glue将数据转换成Iceberg/Huid/Delta Lake保存。



#### 提交TPC-DS测试任务

对应不同的数据格式使用不同的类替换

| 类                                            |               |
| --------------------------------------------- |---------------|
| com.amazonaws.eks.tpcds.BenchmarkSQL          | 无数据格式，Parquet |
| com.amazonaws.eks.tpcds.BenchmarkSQLIceberg   | Iceberg       |
| com.amazonaws.eks.tpcds.BenchmarkSQLDeltalake | Delta Lake    |
| com.amazonaws.eks.tpcds.BenchmarkSQLHudi      | Hudi          |



```shell
SPARK_APPLICATION_ID=<emrserverless-applicationid>
JOB_ROLE_ARN=arn:aws:iam::<accountid>:role/<role-for-emrserverless>
S3_BUCKET=<s3bucket>
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
JOBNAME=SPARK-TPC-DS-Benchmark
RESULT_DIR=SPARK_EMRSERVERLESS_TPCDS-TEST-3T-RESULT

aws emr-serverless start-job-run \
	--application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --name $JOBNAME \
	--job-driver '{
  "sparkSubmit": {
      "entryPoint": "local:///usr/lib/spark/examples/jars/eks-spark-benchmark-assembly-1.0.jar",
      "entryPointArguments":["s3://'${S3_BUCKET}'/banchmark/BLOG_TPCDS-TEST-3T-partitioned","s3://'${S3_BUCKET}'/banchmark/SPARK_EMRSERVERLESS_TPCDS-TEST-3T-RESULT","/opt/tpcds-kit/tools","parquet","3000","1","false","q1-v2.4,q10-v2.4,q11-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q2-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4,q23b-v2.4,q24a-v2.4,q24b-v2.4,q25-v2.4,q26-v2.4,q27-v2.4,q28-v2.4,q29-v2.4,q3-v2.4,q30-v2.4,q31-v2.4,q32-v2.4,q33-v2.4,q34-v2.4,q35-v2.4,q36-v2.4,q37-v2.4,q38-v2.4,q39a-v2.4,q39b-v2.4,q4-v2.4,q40-v2.4,q41-v2.4,q42-v2.4,q43-v2.4,q44-v2.4,q45-v2.4,q46-v2.4,q47-v2.4,q48-v2.4,q49-v2.4,q5-v2.4,q50-v2.4,q51-v2.4,q52-v2.4,q53-v2.4,q54-v2.4,q55-v2.4,q56-v2.4,q57-v2.4,q58-v2.4,q59-v2.4,q6-v2.4,q60-v2.4,q61-v2.4,q62-v2.4,q63-v2.4,q64-v2.4,q65-v2.4,q66-v2.4,q67-v2.4,q68-v2.4,q69-v2.4,q7-v2.4,q70-v2.4,q71-v2.4,q72-v2.4,q73-v2.4,q74-v2.4,q75-v2.4,q76-v2.4,q77-v2.4,q78-v2.4,q79-v2.4,q8-v2.4,q80-v2.4,q81-v2.4,q82-v2.4,q83-v2.4,q84-v2.4,q85-v2.4,q86-v2.4,q87-v2.4,q88-v2.4,q89-v2.4,q9-v2.4,q90-v2.4,q91-v2.4,q92-v2.4,q93-v2.4,q94-v2.4,q95-v2.4,q96-v2.4,q97-v2.4,q98-v2.4,q99-v2.4,ss_max-v2.4","true"],
      "sparkSubmitParameters": "--class <replace>com.amazonaws.eks.tpcds.BenchmarkSQL --conf spark.driver.cores=4 --conf spark.driver.memory=5g --conf spark.executor.cores=4 --conf spark.executor.memory=6g --conf spark.executor.instances=47 --jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://emr-hive-us-east-1-812046859005/pyspark/*.jar"}}' \
  --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
        }
    }
}'
```



## Athena
