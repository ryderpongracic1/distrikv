FROM golang:1.25-alpine AS builder
# GOPROXY passthrough: corporate networks (e.g. Amazon Security DNS filtering)
# block proxy.golang.org. Build with GOPROXY=direct on such networks:
#   GOPROXY=direct docker compose -f docker/docker-compose.yml up -d --build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
# git is required when GOPROXY=direct (module fetch via VCS); builder stage only.
RUN apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o distrikv ./cmd/node

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /build/distrikv .
ENTRYPOINT ["./distrikv"]
