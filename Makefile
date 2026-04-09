IMAGE = ghcr.io/ant-pm/antcc-executor
TAG   = latest

.PHONY: build push

build:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t $(IMAGE):$(TAG) \
		--push \
		executor/

push: build
