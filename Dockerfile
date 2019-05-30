# build stage
FROM golang:1.12.3-alpine AS build-env
RUN apk add --no-cache git
WORKDIR /src/veri
COPY . /src/veri
RUN go build -o veri

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /src/veri/veri /app/
ENTRYPOINT ["/app/veri"]

EXPOSE 8000 10000
