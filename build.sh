#!/usr/bin/env bash

# don't forget to export GOPATH

echo using GOPATH as $GOPATH

if [ "$1" = "local" ]; then
  for GOOS in darwin linux windows; do
    for GOARCH in 386 amd64; do
      echo "Building $GOOS-$GOARCH"
      export GOOS=$GOOS
      export GOARCH=$GOARCH
      if [ "$GOOS" = "windows" ]; then
        go get -u github.com/spf13/cobra
      fi
      CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/veri-$GOOS-$GOARCH
    done
  done
  unset GOOS
  unset GOARCH
else
  docker run --rm -it -v "$GOPATH":/go -w /go/src/github.com/magneticio/verikubistcli dockercore/golang-cross:1.13.15 sh -c '
  for GOOS in darwin linux windows; do
    for GOARCH in 386 amd64; do
      echo "Building $GOOS-$GOARCH"
      export GOOS=$GOOS
      export GOARCH=$GOARCH
      if [ "$GOOS" = "windows" ]; then
        go get -u github.com/spf13/cobra
      fi
      CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/veri-$GOOS-$GOARCH
    done
  done
  '
fi
echo "Binaries can be found under bin directory"
