# build stage
#FROM golang:1.15.6-alpine AS build-env
#FROM golang:1.16.0-alpine3.12 AS build-env
FROM golang:1.16.0-buster AS build-env
RUN apt-get update && apt-get install -y git bash curl build-essential

ENV USER_ID 0

COPY jemalloc.sh .
RUN bash jemalloc.sh

WORKDIR /src/veri
COPY . /src/veri

RUN go mod tidy
RUN go mod download
RUN go mod verify
RUN GOOS=linux GOARCH=amd64 go build -ldflags='-w -s -extldflags "-static"' -tags=jemalloc -a -o veri

# final stage
# FROM gcr.io/distroless/static@sha256:c6d5981545ce1406d33e61434c61e9452dad93ecd8397c41e89036ef977a88f4
# FROM gcr.io/distroless/base-debian10
FROM debian:buster-slim
RUN apt-get update && apt-get install -y libjemalloc-dev
WORKDIR /app
COPY --from=build-env /src/veri/veri /app/
ENTRYPOINT ["/app/veri"]

EXPOSE 8000 10000 6060
