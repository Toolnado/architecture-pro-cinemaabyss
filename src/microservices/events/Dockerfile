FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o events .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/events .
EXPOSE 8082
CMD ["./events"]
