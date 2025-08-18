#
# Copyright 2022 The KubeBlocks Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

################################################################################
# Variables                                                                    #
################################################################################
APP_NAME = dbctl
VERSION ?= 0.1.0-alpha.0
GITHUB_PROXY ?=
INIT_ENV ?= false
TEST_TYPE ?= wesql
GIT_COMMIT  = $(shell git rev-list -1 HEAD)
GIT_VERSION = $(shell git describe --always --abbrev=0 --tag)
# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec


# Go setup
export GO111MODULE = auto
export GOSUMDB = sum.golang.org
export GONOPROXY = github.com/apecloud
export GONOSUMDB = github.com/apecloud
export GOPRIVATE = github.com/apecloud
GO ?= go
GOFMT ?= gofmt
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)
# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell $(GO) env GOBIN))
GOBIN=$(shell $(GO) env GOPATH)/bin
else
GOBIN=$(shell $(GO) env GOBIN)
endif
GOPROXY := $(shell go env GOPROXY)
ifeq ($(GOPROXY),)
GOPROXY := https://proxy.golang.org
## use following GOPROXY settings for Chinese mainland developers.
#GOPROXY := https://goproxy.cn
endif
export GOPROXY


LD_FLAGS="-s -w -X main.version=v${VERSION} -X main.buildDate=`date -u +'%Y-%m-%dT%H:%M:%SZ'` -X main.gitCommit=`git rev-parse HEAD`"
# Which architecture to build - see $(ALL_ARCH) for options.
# if the 'local' rule is being run, detect the ARCH from 'go env'
# if it wasn't specified by the caller.
local : ARCH ?= $(shell go env GOOS)-$(shell go env GOARCH)
ARCH ?= linux-amd64

# build tags
BUILD_TAGS="containers_image_openpgp"


TAG_LATEST ?= false
BUILDX_ENABLED ?= ""
ifeq ($(BUILDX_ENABLED), "")
	ifeq ($(shell docker buildx inspect 2>/dev/null | awk '/Status/ { print $$2 }'), running)
		BUILDX_ENABLED = true
	else
		BUILDX_ENABLED = false
	endif
endif
BUILDX_BUILDER ?= "x-builder"

define BUILDX_ERROR
buildx not enabled, refusing to run this recipe
endef

DOCKER_BUILD_ARGS =
DOCKER_NO_BUILD_CACHE ?= false

ifeq ($(DOCKER_NO_BUILD_CACHE), true)
	DOCKER_BUILD_ARGS = $(DOCKER_BUILD_ARGS) --no-cache
endif


.DEFAULT_GOAL := help

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php
# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: all
all: dbctl ## Make all cmd binaries.

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	$(GOFMT) -l -w -s $$(git ls-files --exclude-standard | grep "\.go$$")

.PHONY: vet
vet: ## Run go vet against code.
	GOOS=$(GOOS) $(GO) vet -tags $(BUILD_TAGS) -mod=mod ./...

.PHONY: lint-fast
lint-fast: staticcheck vet golangci-lint # [INTERNAL] Run all lint job against code.

.PHONY: lint
lint:  ## Run default lint job against code.
	$(MAKE) golangci-lint

.PHONY: golangci-lint
golangci-lint: golangci ## Run golangci-lint against code.
	$(GOLANGCILINT) run ./... --timeout=5m

.PHONY: staticcheck
staticcheck: staticchecktool  ## Run staticcheck against code.
	$(STATICCHECK) -tags $(BUILD_TAGS) ./...

.PHONY: build-checks
build-checks: fmt vet goimports lint-fast ## Run build checks.

.PHONY: mod-download
mod-download: ## Run go mod download against go modules.
	$(GO) mod download

.PHONY: mod-vendor
mod-vendor: module ## Run go mod vendor against go modules.
	$(GO) mod vendor

.PHONY: module
module: ## Run go mod tidy->verify against go modules.
	$(GO) mod tidy -compat=1.21
	$(GO) mod verify

TEST_PACKAGES ?= ./engines/... ./httpserver/... ./operations/...

OUTPUT_COVERAGE=-coverprofile cover.out
.PHONY: test
test: 
	$(GO) test -tags $(BUILD_TAGS) -short $(TEST_PACKAGES)  $(OUTPUT_COVERAGE)

.PHONY: race
race:
	$(GO) test -tags $(BUILD_TAGS) -race $(TEST_PACKAGES)

.PHONY: test-delve
test-delve:  
	dlv --listen=:$(DEBUG_PORT) --headless=true --api-version=2 --accept-multiclient test $(TEST_PACKAGES)

.PHONY: cover-report
cover-report: ## Generate cover.html from cover.out
	$(GO) tool cover -html=cover.out -o cover.html
ifeq ($(GOOS), darwin)
	open ./cover.html
else
	echo "open cover.html with a HTML viewer."
endif

.PHONY: goimports
goimports: goimportstool ## Run goimports against code.
	$(GOIMPORTS) -local github.com/apecloud/dbctl -w $$(git ls-files|grep "\.go$$")


.PHONY: dbctl
dbctl:  build-checks ## Build dbctl binary.
	$(GO) build -tags $(BUILD_TAGS) -ldflags=${LD_FLAGS} -o bin/dbctl ./cmd/dbctl/main.go

.PHONY: run
run: fmt vet 
	$(GO) run ./cmd/dbctl/main.go --config-path ./config/components service

# Run with Delve for development purposes 
# Delve is a debugger for the Go programming language. More info: https://github.com/go-delve/delve
GO_PACKAGE=./cmd/dbctl/main.go
ARGUMENTS=
DEBUG_PORT=2345
run-delve: manifests fmt vet  ## Run Delve debugger.
	dlv --listen=:$(DEBUG_PORT) --headless=true --api-version=2 --accept-multiclient debug $(GO_PACKAGE) -- $(ARGUMENTS)

##@ Contributor

.PHONY: reviewable
reviewable: build-checks test check-license-header ## Run code checks to proceed with PR reviews.
	$(GO) mod tidy -compat=1.21

.PHONY: check-diff
check-diff: reviewable ## Run git code diff checker.
	git --no-pager diff
	git diff --quiet || (echo please run 'make reviewable' to include all changes && false)
	echo branch is clean


##@ Build Dependencies
.PHONY: install-docker-buildx
install-docker-buildx: ## Create `docker buildx` builder.
	@if ! docker buildx inspect $(BUILDX_BUILDER) > /dev/null; then \
		echo "Buildx builder $(BUILDX_BUILDER) does not exist, creating..."; \
		docker buildx create --name=$(BUILDX_BUILDER) --use --driver=docker-container --platform linux/amd64,linux/arm64; \
	else \
		echo "Buildx builder $(BUILDX_BUILDER) already exists"; \
	fi


.PHONY: golangci
golangci: GOLANGCILINT_VERSION = v1.54.2
golangci: ## Download golangci-lint locally if necessary.
ifneq ($(shell which golangci-lint),)
	@echo golangci-lint is already installed
GOLANGCILINT=$(shell which golangci-lint)
else ifeq (, $(shell which $(GOBIN)/golangci-lint))
	@{ \
	set -e ;\
	echo 'installing golangci-lint-$(GOLANGCILINT_VERSION)' ;\
	curl -sSfL $(GITHUB_PROXY)https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOBIN) $(GOLANGCILINT_VERSION) ;\
	echo 'Successfully installed' ;\
	}
GOLANGCILINT=$(GOBIN)/golangci-lint
else
	@echo golangci-lint is already installed
GOLANGCILINT=$(GOBIN)/golangci-lint
endif

.PHONY: staticchecktool
staticchecktool: ## Download staticcheck locally if necessary.
ifeq (, $(shell which staticcheck))
	@{ \
	set -e ;\
	echo 'installing honnef.co/go/tools/cmd/staticcheck' ;\
	go install honnef.co/go/tools/cmd/staticcheck@latest;\
	}
STATICCHECK=$(GOBIN)/staticcheck
else
STATICCHECK=$(shell which staticcheck)
endif

.PHONY: goimportstool
goimportstool: ## Download goimports locally if necessary.
ifeq (, $(shell which goimports))
	@{ \
	set -e ;\
	go install golang.org/x/tools/cmd/goimports@latest ;\
	}
GOIMPORTS=$(GOBIN)/goimports
else
GOIMPORTS=$(shell which goimports)
endif

# NOTE: include must be placed at the end
include docker/docker.mk
