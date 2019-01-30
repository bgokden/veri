# build stage
FROM golang:1.11.2-alpine AS build-env
RUN apk add --no-cache git
WORKDIR /go/src/github.com/bgokden/veri
COPY ./ .
RUN go get && go build -o goapp

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /go/src/github.com/bgokden/veri/goapp /app/
ENTRYPOINT ["/app/goapp"]

EXPOSE 8000 10000
