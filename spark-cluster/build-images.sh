#!/bin/bash

# exit immediately if a command exits with a non-zero status
# i.e. set -e stops the execution of a script if a command or pipeline has an error
set -e

# build images from the dockerfiles
# -t name:tag
docker build -t spark-base:latest ./docker/base
docker build -t spark-master:latest ./docker/spark-master
docker build -t spark-worker:latest ./docker/spark-worker
docker build -t spark-submit:latest ./docker/spark-submit