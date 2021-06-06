#!/usr/bin/env bash

set -e

CURRENT_DIR=$(pwd)
echo "CURRENT_DIR=$CURRENT_DIR"

export REDIS_CLUSTER_IP=0.0.0.0

docker-compose -f docker-compose.yml up -d minio localstack redis mongo elasticsearch redisCluster elasticmq

sleep 50

echo -e "Docker ps."
docker ps

