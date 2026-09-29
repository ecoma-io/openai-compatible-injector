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
# Local builds report "dev"; the release workflow passes the real tag.
ARG VERSION=dev
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
# The probe reads READINESS (GET /readyz), not liveness. That is the fact a
# scheduler needs: a draining instance answers 503 here while GET /healthz
# keeps answering 200, so a rolling restart — or any orchestrator that keys
# off this status — stops routing to a process that is stopping correctly
# instead of mistaking a clean drain for a broken process.
#
#   interval=2s timeout=2s  an observation cadence tight enough to matter
#                           during a rollout, and the cadence the drain's
#                           readiness head start is sized against.
#   start-period=90s        the service may spend up to 30s opening and
#                           migrating the partner-key store and another 30s
#                           doing the same for usage metering, before the
#                           listener exists at all. Failures inside this
#                           window are ignored, so a slow database never
#                           marks a starting container unhealthy.
#   retries=1               one failed probe. This subcommand is served by
#                           this process out of memory — it reads no YAML, no
#                           database, and no upstream — so a failure is a
#                           real answer, and diluting it would delay the
#                           drain notification the head start exists for.
#                           Docker never restarts a container because it is
#                           unhealthy; restart is the `restart:` policy's
#                           business, and a user-defined SIGTERM is unaffected.
HEALTHCHECK --interval=2s --timeout=2s --start-period=90s --retries=1 CMD ["/app/openai-compatible-injector", "healthcheck"]