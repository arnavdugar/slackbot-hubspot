# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=development
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOARCH=$TARGETARCH GOOS=$TARGETOS go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/bot ./cmd/bot && \
    CGO_ENABLED=0 GOARCH=$TARGETARCH GOOS=$TARGETOS go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=development
LABEL org.opencontainers.image.title="Slack HubSpot synchronization bot" \
      org.opencontainers.image.version=$VERSION
COPY --from=build --chown=65532:65532 /out/bot /out/migrate /
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/bot"]
