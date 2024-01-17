#!/bin/bash
# SPDX-FileCopyrightText: Copyright 2021 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0

# cross account test
# "spark.hadoop.fs.s3.bucket.emr-eks-demo-720560070661-us-east-1.customAWSCredentialsProvider": "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider",
# "spark.kubernetes.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN": "arn:aws:iam::720560070661:role/EMRContainers-JobExecutionRole",          
# "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN": "arn:aws:iam::720560070661:role/EMRContainers-JobExecutionRole"   

export EMRCLUSTER_NAME=emr-spark-benchmark-cluster-karpenter
export AWS_REGION=us-east-1
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '$EMRCLUSTER_NAME' && state == 'RUNNING'].id" --output text)
export ROLE_NAME=emr-on-eks-nvme-execution-role
export EMR_EKS_EXECUTION_ARN=arn:aws:iam::812046859005:role/$ROLE_NAME
export EMR_ROLE_ARN=$EMR_EKS_EXECUTION_ARN
export S3BUCKET="tpcds-bemchmark-bucket-812046859005"
export ECR_URL="$ACCOUNTID.dkr.ecr.$AWS_REGION.amazonaws.com"
export RESPO_NAME=eks-emr-spark
export EMR_IMAGE_VERSION_TAG="emr6.14"

aws emr-containers start-job-run \
  --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
  --name emr614-benchmark-karpenter \
  --execution-role-arn $EMR_ROLE_ARN \
  --release-label emr-6.14.0-latest \
  --retry-policy-configuration '{"maxAttempts": 5}' \
  --job-driver '{
  "sparkSubmitJobDriver": {
      "entryPoint": "s3://emr-hive-us-east-1-812046859005/jars/eks-spark-benchmark-assembly-1.0.jar",
      "entryPointArguments":["s3://'$S3BUCKET'/BLOG_TPCDS-TEST-3T-partitioned","s3://'$S3BUCKET'/JDK_EMRONEKS_TPCDS-TEST-3T-RESULT","/opt/tpcds-kit/tools","parquet","3000","1","false","q1-v2.4,q10-v2.4,q11-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q2-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4,q23b-v2.4,q24a-v2.4,q24b-v2.4,q25-v2.4,q26-v2.4,q27-v2.4,q28-v2.4,q29-v2.4,q3-v2.4,q30-v2.4,q31-v2.4,q32-v2.4,q33-v2.4,q34-v2.4,q35-v2.4,q36-v2.4,q37-v2.4,q38-v2.4,q39a-v2.4,q39b-v2.4,q4-v2.4,q40-v2.4,q41-v2.4,q42-v2.4,q43-v2.4,q44-v2.4,q45-v2.4,q46-v2.4,q47-v2.4,q48-v2.4,q49-v2.4,q5-v2.4,q50-v2.4,q51-v2.4,q52-v2.4,q53-v2.4,q54-v2.4,q55-v2.4,q56-v2.4,q57-v2.4,q58-v2.4,q59-v2.4,q6-v2.4,q60-v2.4,q61-v2.4,q62-v2.4,q63-v2.4,q64-v2.4,q65-v2.4,q66-v2.4,q67-v2.4,q68-v2.4,q69-v2.4,q7-v2.4,q70-v2.4,q71-v2.4,q72-v2.4,q73-v2.4,q74-v2.4,q75-v2.4,q76-v2.4,q77-v2.4,q78-v2.4,q79-v2.4,q8-v2.4,q80-v2.4,q81-v2.4,q82-v2.4,q83-v2.4,q84-v2.4,q85-v2.4,q86-v2.4,q87-v2.4,q88-v2.4,q89-v2.4,q9-v2.4,q90-v2.4,q91-v2.4,q92-v2.4,q93-v2.4,q94-v2.4,q95-v2.4,q96-v2.4,q97-v2.4,q98-v2.4,q99-v2.4,ss_max-v2.4","true"],
      "sparkSubmitParameters": "--class com.amazonaws.eks.tpcds.BenchmarkSQL --conf spark.driver.cores=4 --conf spark.driver.memory=5g --conf spark.executor.cores=4 --conf spark.executor.memory=6g --conf spark.executor.instances=47"}}' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.kubernetes.driver.podTemplateFile": "s3://'$S3BUCKET'/app_code/pod-template/karpenter-driver-pod-template.yaml",
          "spark.kubernetes.executor.podTemplateFile": "s3://'$S3BUCKET'/app_code/pod-template/karpenter-executor-pod-template.yaml",
          "spark.kubernetes.driver.limit.cores": "4.1",
          "spark.kubernetes.executor.limit.cores": "4.3",
          "spark.driver.memoryOverhead": "1000",
          "spark.executor.memoryOverhead": "2G",
          "spark.network.timeout": "2000s",
          "spark.executor.heartbeatInterval": "300s",
          
          "spark.ui.prometheus.enabled":"true",
          "spark.executor.processTreeMetrics.enabled":"true",
          "spark.kubernetes.driver.annotation.prometheus.io/scrape":"true",
          "spark.kubernetes.driver.annotation.prometheus.io/path":"/metrics/executors/prometheus/",
          "spark.kubernetes.driver.annotation.prometheus.io/port":"4040",
          "spark.kubernetes.driver.service.annotation.prometheus.io/scrape":"true",
          "spark.kubernetes.driver.service.annotation.prometheus.io/path":"/metrics/driver/prometheus/",
          "spark.kubernetes.driver.service.annotation.prometheus.io/port":"4040",
          "spark.metrics.conf.*.sink.prometheusServlet.class":"org.apache.spark.metrics.sink.PrometheusServlet",
          "spark.metrics.conf.*.sink.prometheusServlet.path":"/metrics/driver/prometheus/",
          "spark.metrics.conf.master.sink.prometheusServlet.path":"/metrics/master/prometheus/",
          "spark.metrics.conf.applications.sink.prometheusServlet.path":"/metrics/applications/prometheus/"
      }},
      {
        "classification": "spark-log4j",
        "properties": {
          "rootLogger.level" : "WARN"
          }
      }
    ], 
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {"logUri": "s3://'$S3BUCKET'/elasticmapreduce/emr-containers"}}}' \
  --tags '{"emronekscost":"karpenter-03"}'
