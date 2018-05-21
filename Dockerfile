# build stage
FROM golang:1.10.2-alpine AS build-env
RUN apk add --no-cache git
WORKDIR /go/src/app
ADD ./ .
RUN go get && go build -o goapp

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /go/src/app/goapp /app/
ENTRYPOINT ["/app/goapp"]

EXPOSE 8000 10000
