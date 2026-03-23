FROM golang:1.26-alpine AS builder

WORKDIR /src/apps/api

COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download

COPY apps/api ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/nexio-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/nexio-worker ./cmd/worker

FROM alpine:3.21 AS api-runtime

RUN addgroup -S app && adduser -S app -G app && apk add --no-cache ca-certificates
COPY --from=builder /out/nexio-api /usr/local/bin/nexio-api

USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/nexio-api"]

FROM alpine:3.21 AS worker-runtime

RUN addgroup -S app && adduser -S app -G app && apk add --no-cache ca-certificates
COPY --from=builder /out/nexio-worker /usr/local/bin/nexio-worker

USER app
ENTRYPOINT ["/usr/local/bin/nexio-worker"]
