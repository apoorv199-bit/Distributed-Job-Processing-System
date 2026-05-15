# Build stage
FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server ./cmd/api

# Run stage — minimal image
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/server /server

EXPOSE 8085
ENTRYPOINT ["/server"]