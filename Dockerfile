# build stage
FROM golang:1.12.3-alpine AS build-env
RUN apk add --no-cache git
WORKDIR /src/veri
COPY . /src/veri
RUN go build -o goapp

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /src/veri/goapp /app/
ENTRYPOINT ["/app/goapp"]

EXPOSE 8000 10000
