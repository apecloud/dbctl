#
#Copyright (C) 2022-2023 ApeCloud Co., Ltd
#
#This file is part of KubeBlocks project
#
#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU Affero General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#This program is distributed in the hope that it will be useful
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU Affero General Public License for more details.
#
#You should have received a copy of the GNU Affero General Public License
#along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# To use buildx: https://github.com/docker/buildx#docker-ce
export DOCKER_CLI_EXPERIMENTAL=enabled

# Debian APT mirror repository
DEBIAN_MIRROR ?=

# Docker image build and push setting
DOCKER:=DOCKER_BUILDKIT=1 docker
DOCKERFILE_DIR?=./docker

# BUILDX_PLATFORMS ?= $(subst -,/,$(ARCH))
BUILDX_PLATFORMS ?= linux/amd64,linux/arm64

# Image URL to use all building/pushing image targets
LORRY_IMG ?= docker.io/apecloud/$(APP_NAME)

# Update whenever you upgrade dev container image
DEV_CONTAINER_VERSION_TAG ?= latest
DEV_CONTAINER_IMAGE_NAME = docker.io/apecloud/$(APP_NAME)-dev

DEV_CONTAINER_DOCKERFILE = Dockerfile-dev
DOCKERFILE_DIR = ./docker
GO_BUILD_ARGS ?= --build-arg GITHUB_PROXY=$(GITHUB_PROXY) --build-arg GOPROXY=$(GOPROXY)
BUILD_ARGS ?=
DOCKER_BUILD_ARGS ?=
DOCKER_BUILD_ARGS += $(GO_BUILD_ARGS) $(BUILD_ARGS)


.PHONY: build-lorry-image
build-lorry-image: install-docker-buildx ## Build lorry container image.
ifneq ($(BUILDX_ENABLED), true)
	$(DOCKER) build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --tag ${LORRY_IMG}:${VERSION} --tag ${LORRY_IMG}:latest
else
ifeq ($(TAG_LATEST), true)
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${LORRY_IMG}:latest
else
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${LORRY_IMG}:${VERSION}
endif
endif

.PHONY: push-lorry-image
push-lorry-image: install-docker-buildx ## Push lorry container image.
ifneq ($(BUILDX_ENABLED), true)
ifeq ($(TAG_LATEST), true)
	$(DOCKER) push ${LORRY_IMG}:latest
else
	$(DOCKER) push ${LORRY_IMG}:${VERSION}
endif
else
ifeq ($(TAG_LATEST), true)
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${LORRY_IMG}:latest --push
else
	$(DOCKER) buildx build . $(DOCKER_BUILD_ARGS) --file $(DOCKERFILE_DIR)/Dockerfile --platform $(BUILDX_PLATFORMS) --tag ${LORRY_IMG}:${VERSION} --push
endif
endif