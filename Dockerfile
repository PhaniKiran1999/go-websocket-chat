# Stage 1: Build
FROM golang:1.25-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files firsr to leverage docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy rest of the source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/server/main.go

# Stage 2: Run
FROM alpine:latest

# Add a non-root user for security
RUN adduser -D appuser
USER appuser

WORKDIR /home/appuser

# Copy only the compiled binary from the builder stage
COPY --from=builder /app/main .

#Expose the port your app runs on
EXPOSE 8080

#Run the binary
CMD ["./main"]