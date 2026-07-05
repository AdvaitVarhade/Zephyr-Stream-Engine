FROM rust:latest

# Install flatbuffers compiler required by Phase 1 build.rs
RUN apt-get update && apt-get install -y flatbuffers-compiler

# Copy the entire workspace
WORKDIR /workspace
COPY . .

# Move to the Phase 2 network ingestion crate
WORKDIR "/workspace/Phase 2/network-ingestion"

# Build the load_bench binary in release mode
RUN cargo build --release --bin load_bench

# Set the command to run the benchmark natively via cargo
CMD ["cargo", "run", "--release", "--bin", "load_bench"]
