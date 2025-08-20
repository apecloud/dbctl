# Build the dbctl image
# includes dbctl and configurations.

## docker buildx build injected build-args:
#BUILDPLATFORM — matches the current machine. (e.g. linux/amd64)
#BUILDOS — os component of BUILDPLATFORM, e.g. linux
#BUILDARCH — e.g. amd64, arm64, riscv64
#BUILDVARIANT — used to set build ARM variant, e.g. v7
#TARGETPLATFORM — The value set with --platform flag on build
#TARGETOS - OS component from --platform, e.g. linux
#TARGETARCH - Architecture from --platform, e.g. arm64
#TARGETVARIANT - used to set target ARM variant, e.g. v7

ARG GO_VERSION=1.23.10-alpine
                                                                                
FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION} as builder

ARG TARGETOS
ARG TARGETARCH
ARG GOPROXY
# ARG GOPROXY=https://goproxy.cn
ARG LD_FLAGS="-s -w"

# ENV GONOPROXY=github.com/apecloud
# ENV GONOSUMDB=github.com/apecloud
# ENV GOPRIVATE=github.com/apecloud
ENV GOPROXY=${GOPROXY}

WORKDIR /src

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="${LD_FLAGS}" -a -o /out/dbctl cmd/dbctl/main.go

# Use alpine with tag 20230329 is corresponding to "edge" tag (latest release to date is 3.18) as of 20230625
FROM docker.io/alpine:3.22 as dist
ARG APK_MIRROR

COPY --from=builder /out/dbctl /bin

USER 65532:65532
