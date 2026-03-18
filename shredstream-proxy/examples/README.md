# Deshred Example

This example demonstrates how to consume transactions from
the shredstream proxy in real-time.

## Prerequisites

- Running shredstream proxy with flag: `--grpc-service-endpoint 127.0.0.1:9999` (TCP) or `--grpc-service-endpoint unix:/tmp/shredstream.sock` (Unix socket)

## Usage

**TCP mode (default):**
```bash
cargo run --example deshred
# Or specify endpoint:
GRPC_ENDPOINT="127.0.0.1:9999" cargo run --example deshred
```

**Unix socket mode:**
```bash
GRPC_ENDPOINT="unix:/tmp/shredstream.sock" cargo run --example deshred
```
