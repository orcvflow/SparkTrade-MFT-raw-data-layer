# Installation Dependencies

## Required Software

### 1. Go (v1.23+)

```bash
# Install Go via snap (recommended)
sudo snap install go --classic

# Verify
go version
```

### 2. Protocol Buffers Compiler (protoc)

```bash
# Install protoc
sudo apt update
sudo apt install -y protobuf-compiler

# Verify
protoc --version
```

### 3. Go Protobuf Plugin

```bash
# Install Go protobuf plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Add to PATH (add to ~/.bashrc)
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 4. ZeroMQ Library

```bash
# Install ZeroMQ development libraries
sudo apt install -y libzmq3-dev

# Verify
pkg-config --modversion libzmq
```

### 5. Build Tools

```bash
# Install build essentials
sudo apt install -y build-essential git
```

---

## Quick Install Script

Run all at once:

```bash
# Update system
sudo apt update

# Install all dependencies
sudo snap install go --classic
sudo apt install -y protobuf-compiler libzmq3-dev build-essential git

# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Add to PATH
echo 'export PATH="$PATH:$(go env GOPATH)/bin"' >> ~/.bashrc
source ~/.bashrc

# Verify installations
go version
protoc --version
pkg-config --modversion libzmq
```

---

## After Installation

```bash
cd ~/raw-data-layer

# Generate Protobuf code
./scripts/generate_proto.sh

# Download Go dependencies
go mod download

# Run tests
go test ./... -v
```

---

## Troubleshooting

### Go not found
```bash
# Check if go is in PATH
which go

# If not, add snap bin to PATH
export PATH="/snap/bin:$PATH"
```

### protoc not found
```bash
# Manual install from GitHub releases
wget https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip
unzip protoc-25.1-linux-x86_64.zip -d $HOME/.local
export PATH="$PATH:$HOME/.local/bin"
```

### ZeroMQ library not found
```bash
# Check if libzmq is installed
ldconfig -p | grep zmq

# If not found, install development package
sudo apt install -y libzmq3-dev pkg-config
```
