# set makefile echo back
ifdef VERBOSE
	V :=
else
	V := @
endif

.PHONY: build
## build : Build binary
build:
	$(V)cargo build

.PHONY: lint
## lint : Lint codespace
lint:
	$(V)cargo clippy --workspace --tests --all-features -- -D warnings

.PHONY: fmt
## fmt : Format all code
fmt:
	$(V)cargo fmt --all -- --check

.PHONY: test
## test : Run test
test:
	$(V)cargo test --workspace

.PHONY: help
## help : Print help message
help: Makefile
	@sed -n 's/^##//p' $< | awk 'BEGIN {FS = ":"} {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
