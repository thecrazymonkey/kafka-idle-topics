FROM golang:1.24 AS builder
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-idle-topics ./cmd/kafka-idle-topics/*

FROM scratch
COPY --from=builder /app/kafka-idle-topics /
# COPY cmd/trustedEntities/LetsEncryptCA.pem /etc/ssl/certs/LetsEncryptCA.pem
ENTRYPOINT ["/kafka-idle-topics", "-kafkaSecurity", "plain_tls"]
