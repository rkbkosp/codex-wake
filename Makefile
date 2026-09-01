.PHONY: build test test-race vet clean

build:
	mkdir -p bin
	go build -trimpath -o bin/codex-wait ./cmd/codex-wait
	go build -trimpath -o bin/codex-waitd ./cmd/codex-waitd
	go build -trimpath -o bin/codex-wait-relay ./cmd/codex-wait-relay

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

clean:
	go clean
