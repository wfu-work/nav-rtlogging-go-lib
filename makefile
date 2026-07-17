SHELL := /bin/sh
.DEFAULT_GOAL := check

GO ?= go
PACKAGES ?= ./...
COVERAGE_FILE ?= coverage.out

.PHONY: all check build fmt fmt-check vet test test-race test-integration coverage benchmark clean

all: check build

check: fmt-check vet test-race

build:
	$(GO) build $(PACKAGES)

fmt:
	gofmt -w ntrip/*.go tcp/*.go

fmt-check:
	@test -z "$$(gofmt -l ntrip/*.go tcp/*.go)" || { \
		echo "Go files need formatting; run 'make fmt'" >&2; \
		gofmt -l ntrip/*.go tcp/*.go >&2; \
		exit 1; \
	}

vet:
	$(GO) vet $(PACKAGES)

test:
	$(GO) test -count=1 $(PACKAGES)

test-race:
	$(GO) test -race -count=1 $(PACKAGES)

test-integration:
	$(GO) test -race -tags=integration ./ntrip -run 'TestEmbeddedCaster.*EndToEnd' -count=1 -timeout=20s

coverage:
	$(GO) test -race -coverprofile=$(COVERAGE_FILE) -count=1 $(PACKAGES)
	$(GO) tool cover -func=$(COVERAGE_FILE)

benchmark:
	$(GO) test -bench=BroadcastLoss -benchmem ./ntrip -run '^$$' -count=1

clean:
	$(GO) clean -testcache
	rm -f $(COVERAGE_FILE)
