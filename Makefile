.PHONY: build check generate image test

VERSION ?= development

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags='-X main.version=$(VERSION)' -o bin/bot ./cmd/bot
	CGO_ENABLED=0 go build -trimpath -o bin/migrate ./cmd/migrate

check:
	go vet ./...
	go test -race ./...

generate:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.7.0 -config api/oapi-codegen.yaml api/openapi.yaml
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

image:
	docker buildx build --build-arg VERSION=$(VERSION) --platform linux/amd64,linux/arm64 --tag slackhubspot:$(VERSION) .

test:
	go test ./...
