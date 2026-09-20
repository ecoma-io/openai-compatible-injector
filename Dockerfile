# Multi-stage build. Runtime stage is `scratch`: exactly one static binary
# plus the CA bundle used to verify HTTPS upstream endpoints. No shell — the
# Docker HEALTHCHECK works because `openai-compatible-injector healthcheck`
# is a binary subcommand of the entrypoint itself.
FROM golang:1.27-alpine AS build
WORKDIR /src
# Dependency resolution is split from the source copy so the module-fetch
# layer survives any source-only edit, and BuildKit cache mounts keep the
# module and compile caches across builds — a rebuild after editing source
# re-downloads nothing and recompiles only what changed. Needs a BuildKit
# builder (Docker Engine default since 23.0, buildx, compose v2); the legacy
# builder rejects --mount loudly rather than misbuilding.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=0.1.0-dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/openai-compatible-injector ./cmd/openai-compatible-injector

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/openai-compatible-injector /app/openai-compatible-injector
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/openai-compatible-injector"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/app/openai-compatible-injector", "healthcheck"]