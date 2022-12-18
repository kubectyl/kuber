# Stage 1 (Build)
FROM golang:1.18-alpine AS builder

ARG VERSION
RUN apk add --update --no-cache git make
WORKDIR /app/
COPY go.mod go.sum /app/
RUN go mod download
COPY . /app/
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w -X github.com/kubectyl/kuber/system.Version=$VERSION" \
    -v \
    -trimpath \
    -o kuber \
    kuber.go
RUN echo "ID=\"distroless\"" > /etc/os-release

# Stage 2 (Final)
FROM gcr.io/distroless/static:latest
COPY --from=builder /etc/os-release /etc/os-release

COPY --from=builder /app/kuber /usr/bin/
CMD [ "/usr/bin/kuber", "--config", "/etc/kubectyl/config.yml" ]

EXPOSE 8080
