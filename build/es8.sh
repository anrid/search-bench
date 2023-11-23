#/bin/bash

docker network create esnetwork

docker run -d --name es8 --net esnetwork -p 9200:9200 -p 9300:9300 \
 -e "discovery.type=single-node" \
 -e "xpack.security.enabled=false" \
 -e "xpack.security.enrollment.enabled=false" \
 elasticsearch:8.11.1
