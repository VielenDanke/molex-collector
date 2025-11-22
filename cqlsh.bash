#!/bin/bash

docker run --rm -it --network moex-collector_moex_collector nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.7'