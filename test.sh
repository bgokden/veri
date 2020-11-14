#!/usr/bin/env bash

go test ./... -v -covermode=count -coverprofile=coverage.out
