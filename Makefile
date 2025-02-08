coordinator: ## Builds the coordinator binary
	@go build -race -o bin/coordinator cmd/coordinator/main.go

worker: ## Builds the worker binary
	@go build -race -o bin/worker cmd/worker/main.go

wc: ## Builds the word counter plugin
	@go build -race -o wc.so -buildmode=plugin cmd/plugins/wc/main.go

indexer: ## Builds the plugin that creates a reverse index for words in a file
	@go build -race -o indexer.so -buildmode=plugin cmd/plugins/indexer/main.go

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
