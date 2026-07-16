SHELL := /bin/sh
.DEFAULT_GOAL := all

GO ?= go
LIB_NAME ?= libRtlogging
BUILD_DIR ?= build

HOST_OS := $(shell $(GO) env GOOS)
HOST_ARCH := $(shell $(GO) env GOARCH)
NATIVE_CC ?= $(shell $(GO) env CC)

DARWIN_AMD64_LIB := $(BUILD_DIR)/darwin/amd64/$(LIB_NAME).dylib
DARWIN_ARM64_LIB := $(BUILD_DIR)/darwin/arm64/$(LIB_NAME).dylib
LINUX_AMD64_LIB := $(BUILD_DIR)/linux/amd64/$(LIB_NAME).so
LINUX_ARM64_LIB := $(BUILD_DIR)/linux/arm64/$(LIB_NAME).so
WINDOWS_AMD64_LIB := $(BUILD_DIR)/windows/amd64/$(LIB_NAME).dll
WINDOWS_AMD64_EXE := $(BUILD_DIR)/windows/amd64/rtlogging.exe

DARWIN_CC ?= clang
LINUX_AMD64_CC ?= x86_64-linux-gnu-gcc
LINUX_ARM64_CC ?= aarch64-linux-gnu-gcc
WINDOWS_AMD64_CC ?= x86_64-w64-mingw32-gcc

# Native builds use the compiler configured for the local Go/CGO environment.
# Cross builds keep the target-specific compilers above.
ifeq ($(HOST_OS)/$(HOST_ARCH),linux/amd64)
LINUX_AMD64_CC := $(NATIVE_CC)
endif
ifeq ($(HOST_OS)/$(HOST_ARCH),linux/arm64)
LINUX_ARM64_CC := $(NATIVE_CC)
endif
ifeq ($(HOST_OS)/$(HOST_ARCH),windows/amd64)
WINDOWS_AMD64_CC := $(NATIVE_CC)
endif

.PHONY: all build-current-platform build-macos build-linux build-windows build-darwin
.PHONY: build-darwin-amd64 build-darwin-arm64 build-linux-amd64 build-linux-arm64
.PHONY: build-windows-amd64 build-windows-exe build-all clean

all: build-current-platform

ifeq ($(HOST_OS),darwin)
build-current-platform: build-macos
else ifeq ($(HOST_OS),linux)
build-current-platform: build-linux
else ifeq ($(HOST_OS),windows)
build-current-platform: build-windows
else
build-current-platform:
	@echo "Unsupported host platform: $(HOST_OS)/$(HOST_ARCH)" >&2
	@exit 1
endif

# Convenience aliases use the platform/architecture output layout below.
build-macos: build-darwin-$(HOST_ARCH)

build-linux: build-linux-$(HOST_ARCH)

build-windows: build-windows-amd64

build-darwin: build-darwin-amd64

build-darwin-amd64:
	@if [ "$(HOST_OS)" != "darwin" ]; then echo "Darwin CGO builds require a macOS SDK and host" >&2; exit 1; fi
	@command -v "$(DARWIN_CC)" >/dev/null 2>&1 || { echo "Missing compiler: $(DARWIN_CC)" >&2; exit 1; }
	@echo "Building Darwin amd64 shared library..."
	@mkdir -p "$(dir $(DARWIN_AMD64_LIB))"
	GOOS=darwin GOARCH=amd64 CC="$(DARWIN_CC)" CGO_ENABLED=1 CGO_CFLAGS="-arch x86_64" CGO_LDFLAGS="-arch x86_64" $(GO) build -buildmode=c-shared -buildvcs=false -o "$(DARWIN_AMD64_LIB)" .

build-darwin-arm64:
	@if [ "$(HOST_OS)" != "darwin" ]; then echo "Darwin CGO builds require a macOS SDK and host" >&2; exit 1; fi
	@command -v "$(DARWIN_CC)" >/dev/null 2>&1 || { echo "Missing compiler: $(DARWIN_CC)" >&2; exit 1; }
	@echo "Building Darwin arm64 shared library..."
	@mkdir -p "$(dir $(DARWIN_ARM64_LIB))"
	GOOS=darwin GOARCH=arm64 CC="$(DARWIN_CC)" CGO_ENABLED=1 CGO_CFLAGS="-arch arm64" CGO_LDFLAGS="-arch arm64" $(GO) build -buildmode=c-shared -buildvcs=false -o "$(DARWIN_ARM64_LIB)" .

build-linux-amd64:
	@command -v "$(LINUX_AMD64_CC)" >/dev/null 2>&1 || { echo "Missing compiler: $(LINUX_AMD64_CC)" >&2; exit 1; }
	@echo "Building Linux amd64 shared library..."
	@mkdir -p "$(dir $(LINUX_AMD64_LIB))"
	GOOS=linux GOARCH=amd64 CC="$(LINUX_AMD64_CC)" CGO_ENABLED=1 $(GO) build -buildmode=c-shared -buildvcs=false -o "$(LINUX_AMD64_LIB)" .

build-linux-arm64:
	@command -v "$(LINUX_ARM64_CC)" >/dev/null 2>&1 || { echo "Missing compiler: $(LINUX_ARM64_CC)" >&2; exit 1; }
	@echo "Building Linux arm64 shared library..."
	@mkdir -p "$(dir $(LINUX_ARM64_LIB))"
	GOOS=linux GOARCH=arm64 CC="$(LINUX_ARM64_CC)" CGO_ENABLED=1 $(GO) build -buildmode=c-shared -buildvcs=false -o "$(LINUX_ARM64_LIB)" .

build-windows-amd64:
	@command -v "$(WINDOWS_AMD64_CC)" >/dev/null 2>&1 || { echo "Missing compiler: $(WINDOWS_AMD64_CC)" >&2; exit 1; }
	@echo "Building Windows amd64 shared library..."
	@mkdir -p "$(dir $(WINDOWS_AMD64_LIB))"
	GOOS=windows GOARCH=amd64 CC="$(WINDOWS_AMD64_CC)" CGO_ENABLED=1 $(GO) build -buildmode=c-shared -trimpath -ldflags="-s -w" -buildvcs=false -o "$(WINDOWS_AMD64_LIB)" .

build-windows-exe:
	@echo "Building Windows amd64 CLI executable..."
	@mkdir -p "$(dir $(WINDOWS_AMD64_EXE))"
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -buildvcs=false -o "$(WINDOWS_AMD64_EXE)" ./cmd

build-all: build-darwin-amd64 build-darwin-arm64 build-linux-amd64 build-linux-arm64 build-windows-amd64 build-windows-exe

clean:
	@echo "Cleaning $(BUILD_DIR)/..."
	rm -rf "$(BUILD_DIR)"
