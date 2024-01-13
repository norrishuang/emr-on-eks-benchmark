export FLINK_HOME=/opt/flink-1.17.1
export NAMESPACE=emr-eks-flink
export CLUSTER_ID=flink-test-03 #flink 集群ID，自定义
export IMAGE=812046859005.dkr.ecr.us-east-1.amazonaws.com/eks-flink:emr6.13-app-1.2
export FLINK_SERVICE_ACCOUNT=emr-containers-sa-flink
export FLINK_CLUSTER_ROLE_BINDING=emr-containers-crb-flink

$FLINK_HOME/bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.namespace=$NAMESPACE \
    -Dstate.checkpoints.dir=s3://emr-hive-us-east-1-812046859005/flink-eks/checkpoints/ \
    -Dstate.savepoints.dir=s3://emr-hive-us-east-1-812046859005/flink-eks/savepoints/ \
    -Dtaskmanager.numberOfTaskSlots=2 \
    -Dparallelism=16 \
    -Dkubernetes.cluster-id=$CLUSTER_ID \
    -Dkubernetes.container.image.ref=$IMAGE \
    -Dkubernetes.service-account=$FLINK_SERVICE_ACCOUNT \
    -Dkubernetes.operator.job.autoscaler.enabled=true \
    -Dkubernetes.operator.job.autoscaler.stabilization.interval=1m \
    -Dkubernetes.operator.job.autoscaler.metrics.window=5m \
    -Dkubernetes.operator.job.autoscaler.target.utilization=0.6 \
    -Dkubernetes.operator.job.autoscaler.target.utilization.boundary=0.2 \
    -Dkubernetes.operator.job.autoscaler.restart.time=2m \
    -Dkubernetes.operator.job.autoscaler.catch-up.duration=5m \
    local:///opt/flink/usrlib/aws-flink-s3sink-1.2.jar