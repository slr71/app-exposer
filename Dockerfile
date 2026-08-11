### Build stage
FROM golang:1.26 AS builder

WORKDIR /build

# Install swag for swagger documentation generation
RUN go install github.com/swaggo/swag/cmd/swag@latest

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binaries; let the build platform determine GOOS/GOARCH.
ENV CGO_ENABLED=0

RUN go build -ldflags='-w -s' -o app-exposer cmd/app-exposer/*.go
RUN go build -ldflags='-w -s' -o vice-operator cmd/vice-operator/*.go

# Generate swagger documentation for both binaries (matches Justfile)
RUN swag init --parseDependency -g app.go -d cmd/app-exposer/,httphandlers/,common/,incluster/
RUN swag init --parseDependency -g app.go -d cmd/vice-operator/,operator/,operatorclient/,common/ -o operatordocs/ --instanceName operator --td '[{,}]'

### Final stage - minimal runtime image
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Copy the binaries from builder
COPY --from=builder /build/app-exposer /app-exposer
COPY --from=builder /build/vice-operator /vice-operator

# Copy swagger documentation for both app-exposer and vice-operator
COPY --from=builder /build/docs/swagger.json /docs/swagger.json
COPY --from=builder /build/operatordocs/operator_swagger.json /operatordocs/operator_swagger.json

EXPOSE 60000

ENTRYPOINT ["/app-exposer"]
