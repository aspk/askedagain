CUR_DIR=$(dirname ${BASH_SOURCE})

CLUSTER_NAME=kellie-cassandra-cluster

peg up ${CUR_DIR}/cassandra_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment
peg install ${CLUSTER_NAME} cassandra

# wait 

peg service ${CLUSTER_NAME} cassandra start