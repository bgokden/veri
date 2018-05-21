#!/usr/bin/env bash

VERSION=0.0.1

docker build -t berkgokden/veri:$VERSION .
docker tag berkgokden/veri:$VERSION berkgokden/veri
