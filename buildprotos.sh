#!/usr/bin/env bash

# protoc -I veriservice/ veriservice/veriservice.proto --go_out=plugins=grpc:veriservice

python -m grpc_tools.protoc -I veriservice/ --python_out=client/python/veriservice --grpc_python_out=client/python/veriservice veriservice/veriservice.proto
