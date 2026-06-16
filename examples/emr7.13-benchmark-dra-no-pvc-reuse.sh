#!/bin/bash
# EMR 7.13 on EKS - TPC-DS 3TB Benchmark
# DRA enabled WITHOUT PVC Reuse + KubernetesLocalDiskShuffleDataIO (baseline)
# Instance: 7x c8g.8xlarge, Executor: 4 cores, 6g memory, 47 instances (max)

export AWS_REGION="us-east-1"
export EMRCLUSTER_NAME="emr-spark-cluster"
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '$EMRCLUSTER_NAME' && state == 'RUNNING'].id" --output text)
export EMR_ROLE_ARN="arn:aws:iam::$ACCOUNTID:role/emr-on-eks-nvme-execution-role"
export S3BUCKET="tpcds-bemchmark-bucket-812046859005"
export JAR_PATH="s3://emr-hive-us-east-1-812046859005/jars/eks-spark-benchmark-assembly-1.0.jar"

echo "VIRTUAL_CLUSTER_ID: $VIRTUAL_CLUSTER_ID"

aws emr-containers start-job-run \
  --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
  --name emr713-tpcds-dra-no-pvc-reuse \
  --execution-role-arn $EMR_ROLE_ARN \
  --release-label emr-7.13.0-latest \
  --retry-policy-configuration '{"maxAttempts": 1}' \
  --job-driver '{
  "sparkSubmitJobDriver": {
      "entryPoint": "'$JAR_PATH'",
      "entryPointArguments":["s3://'$S3BUCKET'/BLOG_TPCDS-TEST-3T-partitioned","s3://'$S3BUCKET'/EMRONEKS_TPCDS-TEST-3T-RESULT-DRA-NO-PVC-REUSE","/opt/tpcds-kit/tools","parquet","3000","1","false","q1-v2.4,q10-v2.4,q11-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q2-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4,q23b-v2.4,q24a-v2.4,q24b-v2.4,q25-v2.4,q26-v2.4,q27-v2.4,q28-v2.4,q29-v2.4,q3-v2.4,q30-v2.4,q31-v2.4,q32-v2.4,q33-v2.4,q34-v2.4,q35-v2.4,q36-v2.4,q37-v2.4,q38-v2.4,q39a-v2.4,q39b-v2.4,q4-v2.4,q40-v2.4,q41-v2.4,q42-v2.4,q43-v2.4,q44-v2.4,q45-v2.4,q46-v2.4,q47-v2.4,q48-v2.4,q49-v2.4,q5-v2.4,q50-v2.4,q51-v2.4,q52-v2.4,q53-v2.4,q54-v2.4,q55-v2.4,q56-v2.4,q57-v2.4,q58-v2.4,q59-v2.4,q6-v2.4,q60-v2.4,q61-v2.4,q62-v2.4,q63-v2.4,q64-v2.4,q65-v2.4,q66-v2.4,q67-v2.4,q68-v2.4,q69-v2.4,q7-v2.4,q70-v2.4,q71-v2.4,q72-v2.4,q73-v2.4,q74-v2.4,q75-v2.4,q76-v2.4,q77-v2.4,q78-v2.4,q79-v2.4,q8-v2.4,q80-v2.4,q81-v2.4,q82-v2.4,q83-v2.4,q84-v2.4,q85-v2.4,q86-v2.4,q87-v2.4,q88-v2.4,q89-v2.4,q9-v2.4,q90-v2.4,q91-v2.4,q92-v2.4,q93-v2.4,q94-v2.4,q95-v2.4,q96-v2.4,q97-v2.4,q98-v2.4,q99-v2.4,ss_max-v2.4","true"],
      "sparkSubmitParameters": "--class com.amazonaws.eks.tpcds.BenchmarkSQL --conf spark.driver.cores=4 --conf spark.driver.memory=5g --conf spark.executor.cores=4 --conf spark.executor.memory=6g --conf spark.executor.instances=47"}}' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
          "spark.executor.memoryOverhead": "2G",
          "spark.network.timeout": "2000s",
          "spark.executor.heartbeatInterval": "300s",

          "spark.dynamicAllocation.enabled": "true",
          "spark.dynamicAllocation.shuffleTracking.enabled": "true",
          "spark.dynamicAllocation.minExecutors": "1",
          "spark.dynamicAllocation.maxExecutors": "47",
          "spark.dynamicAllocation.executorIdleTimeout": "60s",

          "spark.kubernetes.executor.node.selector.node.kubernetes.io/instance-type": "c7i.8xlarge"
        }
      },
      {
        "classification": "spark-log4j",
        "properties": {
          "rootLogger.level": "WARN"
        }
      }
    ],
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {"logUri": "s3://'$S3BUCKET'/elasticmapreduce/emr-containers"}}}'
