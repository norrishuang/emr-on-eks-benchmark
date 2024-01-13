ARG FLINK_BASE_IMAGE=public.ecr.aws/emr-on-eks/flink/emr-6.13.0-flink:latest

FROM ${FLINK_BASE_IMAGE}
USER root

RUN mkdir -p $FLINK_HOME/usrlib

COPY aws-flink-s3sink-1.1.jar $FLINK_HOME/usrlib/aws-flink-s3sink-1.1.jar
# # Use hadoop user and group 
USER hadoop:hadoop