#!/usr/bin/env bash

# export PATH=$PATH:$GOPATH/bin
# protoc -I veriservice/ veriservice/veriservice.proto --go_out=plugins=grpc:veriservice

protoc -I veriservice/ veriservice/veriservice.proto --go_out=plugins=grpc:veriservice