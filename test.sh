#!/usr/bin/env bash

go env -w GOFLAGS=-mod=mod

go test ./... -v -covermode=count -coverprofile=coverage.out
