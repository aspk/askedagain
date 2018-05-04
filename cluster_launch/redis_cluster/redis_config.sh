PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=kellie-redis-cluster

peg up ${PEG_ROOT}/examples/redis/master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} redis

peg sshcmd-cluster ${CLUSTER_NAME} "pip install smart_open kafka pyspark boto3 botocore termcolor"