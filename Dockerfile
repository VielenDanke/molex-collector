# --- Stage 1: Builder ---
# Use the official Go image to build the app
FROM golang:1.25-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker layer caching
# (Dependencies will only re-download if these files change)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application
# CGO_ENABLED=0: Forces static linking (crucial for Alpine/Scratch)
# GOOS=linux: Ensures linux binary
RUN CGO_ENABLED=0 GOOS=linux go build -o moex-collector

# --- Stage 2: Runner ---
# Use a tiny base image for the final container
FROM alpine:latest

WORKDIR /app

# Install CA certificates (Required if making HTTPS or SASL/SSL calls)
RUN apk --no-cache add ca-certificates

# Copy only the compiled binary from the builder stage
COPY --from=builder /app/moex-collector .

# (Optional) If your app uses a .env file or config, copy it here
# COPY .env .

# Command to run the executable
CMD ["./moex-collector"]