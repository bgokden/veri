#!/usr/bin/env bash

docker network create -d bridge mynetwork

docker rm -f service1
docker run -d -p 8000:8000 -p 10000:10000 --network=mynetwork --name=service1 berkgokden/veri serve -v
sleep 60

if [[ "3" == "$1" ]]; then
  docker rm -f service2
  docker run -d --network=mynetwork --name=service2 berkgokden/veri serve -v --services=service1:10000
  sleep 60

  docker rm -f service3
  docker run -d --network=mynetwork --name=service3 berkgokden/veri serve -v --services=service1:10000
  sleep 60
fi

# docker rm -f service4
# docker run -d --network=mynetwork --name=service4 berkgokden/veri serve -v --services=service1:10000
# sleep 5

# docker rm -f service5
# docker run -d --network=mynetwork --name=service5 berkgokden/veri serve -v --services=service1:10000
# sleep 5
echo "done"
