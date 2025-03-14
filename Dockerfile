### First stage
FROM quay.io/goswagger/swagger AS swagger
FROM harbor.cyverse.org/de/just:latest AS just
FROM golang:1.24 AS build-root

COPY --from=just /usr/local/cargo/bin/just /usr/bin/

WORKDIR /go/src/github.com/cyverse-de/app-exposer

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN just app-exposer
RUN cp ./bin/app-exposer /bin/app-exposer

COPY --from=swagger /usr/bin/swagger /usr/bin/
RUN swagger generate spec -o ./docs/swagger.json --scan-models

ENTRYPOINT ["app-exposer"]

EXPOSE 60000
