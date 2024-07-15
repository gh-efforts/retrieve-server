 SHELL=/usr/bin/env bash

 all: build
.PHONY: all

unexport GOFLAGS

ldflags=-X=github.com/gh-efforts/retrieve-server/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"

build: retrieve-server
.PHONY: build

retrieve-server:
	rm -f retrieve-server
	go build $(GOFLAGS) -o retrieve-server ./cmd/retrieve-server
.PHONY: retrieve-server

clean:
	rm -f retrieve-server
	go clean
.PHONY: clean