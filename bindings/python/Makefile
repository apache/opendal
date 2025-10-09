.DEFAULT_GOAL := help

# Project Root
ROOT_DIR := $(shell pwd)

SHELL = bash
VENV ?= ./.venv

# Detect Windows vs Unix
ifeq ($(OS),Windows_NT)
    VENV_BIN := $(VENV)/Scripts
else
    VENV_BIN := $(VENV)/bin
endif

# ---- Commands ----
PYTHON := $(VENV_BIN)/python
UV := $(VENV_BIN)/uv
RUFF := $(VENV_BIN)/ruff
MATURIN := $(VENV_BIN)/maturin

.venv:  ## Set up Python virtual environment and install requirements
	python3 -m venv $(VENV)
	@$(MAKE) requirements

# Note: `greenify` is installed separately as it does not have wheels, causing --no-build to fail.
.PHONY: requirements
requirements: .venv  ## Install/refresh Python project requirements
	@unset CONDA_PREFIX \
	&& $(PYTHON) -m pip install --upgrade pip uv \
	&& $(UV) sync --frozen --all-groups --all-extras --no-install-workspace --compile-bytecode --no-build --no-install-package greenify \
	&& $(UV) pip install --compile-bytecode greenify

.PHONY: stub-gen
stub-gen: ## Generate Python type stubs using stub_gen
	just generate python
	cargo run --bin stub_gen $(ARGS)
	@$(MAKE) fmt

.PHONY: build
build: .venv stub-gen ## Compile and produce development wheel
	@unset CONDA_PREFIX \
	&& $(MATURIN) build -m ./Cargo.toml $(ARGS)

.PHONY: build-release
build-release: .venv stub-gen ## Compile and produce release wheel with super slow extra optimization turned on
	@unset CONDA_PREFIX \
	&& $(MATURIN) build -m ./Cargo.toml --profile release --strip --release $(ARGS)

.PHONY: install
install: .venv stub-gen ## Compile and install development wheel in current venv
	@unset CONDA_PREFIX \
	&& $(MATURIN) develop -m ./Cargo.toml $(ARGS)

.PHONY: install-release
install-release: .venv stub-gen  ## Compile and install release wheel in current venv with super slow extra optimization turned on
	@unset CONDA_PREFIX \
	&& $(MATURIN) develop -m ./Cargo.toml --profile release --strip --release $(ARGS)

.PHONY: fix
fix: .venv  ## Fix lint commands issues in Rust and Python
	cargo clippy --workspace --all-targets --all-features --fix
	$(RUFF) check --fix
	$(MAKE) fmt

.PHONY: lint
lint: .venv  ## Run lint checks for Rust and Python
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings -D clippy::dbg_macro
	$(RUFF) check
	$(VENV_BIN)/mbake format --check Makefile
	$(VENV_BIN)/mbake validate Makefile
	$(VENV_BIN)/dprint check

.PHONY: fmt
fmt: .venv  ## Format Rust and Python
	cargo fmt --all
	$(RUFF) format
	$(VENV_BIN)/dprint fmt
	$(VENV_BIN)/mbake format Makefile

.PHONY: pre-commit
pre-commit: fmt lint  ## Run all code formatting and lint/ quality checks

.PHONY: clean
clean: ## Clean up caches and build artifacts
	-$(RUFF) clean
	cargo clean --quiet
	rm -rf $(VENV)
	rm -rf build/ dist/
	rm -rf .pytest_cache/ .mypy_cache/ .hypothesis/
	find . -type f -name '*.py[co]' -delete -or -type d -name __pycache__ -exec rm -r {} +

.PHONY: help
help: ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort