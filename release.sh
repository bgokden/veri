#!/usr/bin/env bash

set -e

VERSION=$(go run main.go version --clean)

git tag -a ${VERSION} -m "Release for version ${VERSION}"

git push origin ${VERSION}
