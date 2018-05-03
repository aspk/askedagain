CUR_DIR=$(dirname ${BASH_SOURCE})

CLUSTER_NAME=kellie-kafka-cluster

peg up ${CUR_DIR}/kafka_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka

# wait 
peg service ${CLUSTER_NAME} zookeeper start
peg service ${CLUSTER_NAME} kafka start

peg