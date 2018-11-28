PACKAGES=$(shell go list ./... | grep -v '/vendor/')

VERSION := $(shell git describe --tags --long | sed 's/v\(.*\)/\1/')
BUILD_FLAGS = -ldflags "-X terra/version.Version=${VERSION}"

all: get_tools get_vendor_deps build test

get_tools:
	go get github.com/golang/dep/cmd/dep

build:
	go build $(BUILD_FLAGS) -o bin/terracli ./cmd/terracli
	go build $(BUILD_FLAGS) -o bin/terrad ./cmd/terrad

install:
	go install $(BUILD_FLAGS) ./cmd/terracli
	go install $(BUILD_FLAGS) ./cmd/terrad

get_vendor_deps:
	@rm -rf vendor/
	@dep ensure

test:
	@go test $(PACKAGES)

benchmark:
	@go test -bench=. $(PACKAGES)


########################################
### Local validator nodes using docker and docker-compose

build-linux:
	LEDGER_ENABLED=false GOOS=linux GOARCH=amd64 $(MAKE) build

build-docker-terradnode:
	$(MAKE) -C networks/local

# Run a 4-node testnet locally
localnet-start: localnet-stop
	@if ! [ -f bin/node0/terrad/config/genesis.json ]; then docker run --rm -v $(CURDIR)/bin:/terrad:Z tendermint/terradnode testnet --v 4 -o . --starting-ip-address 192.168.10.2 ; fi
	docker-compose up -d

# Stop testnet
localnet-stop:
	docker-compose down

.PHONY: all build test benchmark build-linux build-docker-terradnode localnet-start localnet-stop