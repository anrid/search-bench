#/bin/bash

docker network create mcnetwork

docker run -d --name manticore --net mcnetwork -p 9306:9306 -p 9308:9308 -p 9312:9312 \
 -e EXTRA=1 \
 manticoresearch/manticore:6.2.12
