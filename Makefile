SHELL := /bin/bash

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: goimports
goimports:
	goimports -w  .

.PHONY: test
test:
	go test -race -count=1 -v ./...
