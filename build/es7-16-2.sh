#/bin/bash

docker network create esnetwork
docker run -d --name es7-16-2 --net esnetwork -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.16.2
