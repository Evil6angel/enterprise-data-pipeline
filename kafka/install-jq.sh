#!/bin/bash
docker exec -it kafka-connect apt-get update
docker exec -it kafka-connect apt-get install -y jq
