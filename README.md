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
