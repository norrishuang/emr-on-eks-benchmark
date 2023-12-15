#!/bin/bash
# SPDX-FileCopyrightText: Copyright 2021 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0

export EMRCLUSTER_NAME=emr-spark-benchmark-cluster-karpenter
export AWS_REGION=us-east-1
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '$EMRCLUSTER_NAME' && state == 'RUNNING'].id" --output text)
export ROLE_NAME=emr-on-eks-nvme-execution-role
export EMR_EKS_EXECUTION_ARN=arn:aws:iam::812046859005:role/$ROLE_NAME
export EMR_ROLE_ARN=$EMR_EKS_EXECUTION_ARN
export S3BUCKET="emr-hive-us-east-1-812046859005"
export ECR_URL="$ACCOUNTID.dkr.ecr.$AWS_REGION.amazonaws.com"
export RESPO_NAME=eks-emr-spark
export EMR_IMAGE_VERSION_TAG="emr6.14"

# export IMAGE=$ECR_URL/$RESPO_NAME:$EMR_IMAGE_VERSION_TAG
# 使用EMR原生的IMAGE
export IMAGE="755674844232.dkr.ecr.us-east-1.amazonaws.com/spark/emr-6.14.0:latest"
export JAR_PATH=s3://emr-hive-us-east-1-812046859005/jars/eks-spark-benchmark-assembly-1.0.jar
export SERVICE_ACCOUNTNAME=emr-job-execution-sa
export NAMESPACE=emr-karpenter


cat <<EOF >emr-spark-operator-example.yaml
---
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: tpcds-example
  namespace: $NAMESPACE
spec:
  type: Scala
  mode: cluster
  # EMR optimized runtime image
  image: $IMAGE
  imagePullPolicy: Always
  mainClass: com.amazonaws.eks.tpcds.BenchmarkSQL
  mainApplicationFile: $JAR_PATH
  arguments:
    - "s3://$S3BUCKET/banchmark/BLOG_TPCDS-TEST-3T-partitioned"
    - "s3://$S3BUCKET/banchmark/JDK_EMRONEKS_TPCDS-TEST-3T-RESULT"
    - "/opt/tpcds-kit/tools"
    - "parquet"
    - "3000"
    - "1"
    - "false"
    - "q1-v2.4,q10-v2.4,q11-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q2-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4,q23b-v2.4,q24a-v2.4,q24b-v2.4,q25-v2.4,q26-v2.4,q27-v2.4,q28-v2.4,q29-v2.4,q3-v2.4,q30-v2.4,q31-v2.4,q32-v2.4,q33-v2.4,q34-v2.4,q35-v2.4,q36-v2.4,q37-v2.4,q38-v2.4,q39a-v2.4,q39b-v2.4,q4-v2.4,q40-v2.4,q41-v2.4,q42-v2.4,q43-v2.4,q44-v2.4,q45-v2.4,q46-v2.4,q47-v2.4,q48-v2.4,q49-v2.4,q5-v2.4,q50-v2.4,q51-v2.4,q52-v2.4,q53-v2.4,q54-v2.4,q55-v2.4,q56-v2.4,q57-v2.4,q58-v2.4,q59-v2.4,q6-v2.4,q60-v2.4,q61-v2.4,q62-v2.4,q63-v2.4,q64-v2.4,q65-v2.4,q66-v2.4,q67-v2.4,q68-v2.4,q69-v2.4,q7-v2.4,q70-v2.4,q71-v2.4,q72-v2.4,q73-v2.4,q74-v2.4,q75-v2.4,q76-v2.4,q77-v2.4,q78-v2.4,q79-v2.4,q8-v2.4,q80-v2.4,q81-v2.4,q82-v2.4,q83-v2.4,q84-v2.4,q85-v2.4,q86-v2.4,q87-v2.4,q88-v2.4,q89-v2.4,q9-v2.4,q90-v2.4,q91-v2.4,q92-v2.4,q93-v2.4,q94-v2.4,q95-v2.4,q96-v2.4,q97-v2.4,q98-v2.4,q99-v2.4,ss_max-v2.4"
    - "true"
  hadoopConf:
    # EMRFS filesystem config
    fs.s3.customAWSCredentialsProvider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider
    fs.s3.impl: com.amazon.ws.emr.hadoop.fs.EmrFileSystem
    fs.AbstractFileSystem.s3.impl: org.apache.hadoop.fs.s3.EMRFSDelegate
    fs.s3.buffer.dir: /mnt/s3
    fs.s3.getObject.initialSocketTimeoutMilliseconds: "2000"
    mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem: "2"
    mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem: "true"
  sparkConf:
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "s3://$S3BUCKET/"
    spark.kubernetes.driver.pod.name: driver-spark-tpcds
    # Required for EMR Runtime and Glue Catalogue
    spark.driver.extraClassPath: /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/home/hadoop/extrajars/*
    spark.driver.extraLibraryPath: /usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native
    spark.executor.extraClassPath: /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/home/hadoop/extrajars/*
    spark.executor.extraLibraryPath: /usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native
    # EMRFS commiter
    spark.sql.parquet.output.committer.class: com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter
    spark.sql.parquet.fs.optimized.committer.optimization-enabled: "true"
    spark.sql.emr.internal.extensions: com.amazonaws.emr.spark.EmrSparkSessionExtensions
    spark.executor.defaultJavaOptions: -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParallelGC -XX:InitiatingHeapOccupancyPercent=70 -XX:OnOutOfMemoryError='kill -9 %p'
    spark.driver.defaultJavaOptions:  -XX:OnOutOfMemoryError='kill -9 %p' -XX:+UseParallelGC -XX:InitiatingHeapOccupancyPercent=70
    spark.kubernetes.driver.podTemplateFile: "s3://$S3BUCKET/app_code/pod-template/karpenter-driver-pod-template.yaml"
    spark.kubernetes.executor.podTemplateFile: "s3://$S3BUCKET/app_code/pod-template/karpenter-executor-pod-template.yaml"
    # 如果通过 pod template,下面这两个参数必须
    spark.kubernetes.driver.podTemplateContainerName: spark-kubernetes-driver
    spark.kubernetes.executor.podTemplateContainerName: spark-kubernetes-executor
    # node decommission
    spark.decommission.enabled: "true"
    spark.storage.decommission.rddBlocks.enabled": "true"
    spark.storage.decommission.shuffleBlocks.enabled" : "true"
    spark.storage.decommission.enabled": "true"
    spark.storage.decommission.fallbackStorage.path": "s3://$S3BUCKET/banchmark/decommission/"
  sparkVersion: "3.3.1"
  restartPolicy:
    type: Never
  driver:
    cores: 4
    memory: "5g"
    serviceAccount: $SERVICE_ACCOUNTNAME
  executor:
    cores: 4
    memory: "6g"
    instances: 47
    serviceAccount: $SERVICE_ACCOUNTNAME
EOF

kubectl apply -f emr-spark-operator-example.yaml