.DEFAULT_GOAL := help

# ==================================================================================== #
# Project Configuration
# ==================================================================================== #
ROOT_DIR := $(shell pwd)
SHELL := bash
VENV ?= ./.venv

# Detect OS to set the correct binary path within the virtual environment
ifeq ($(OS),Windows_NT)
    VENV_BIN := $(VENV)/Scripts
else
    VENV_BIN := $(VENV)/bin
endif

# Tool executables
PYTHON := $(VENV_BIN)/python
UV := $(VENV_BIN)/uv
RUFF := $(VENV_BIN)/ruff
MATURIN := $(VENV_BIN)/maturin

# ==================================================================================== #
# Phony Targets
# ==================================================================================== #
.PHONY: help setup requirements install install-release develop develop-release \
		build build-release stub-gen lint fmt fix pre-commit clean

# ==================================================================================== #
# Setup & Installation
# ==================================================================================== #
setup: .venv ## Set up the development environment

.venv:
	@echo "--- Setting up Python virtual environment... ---"
	@if [ ! -d "$(VENV)" ]; then python3 -m venv $(VENV); fi
	@$(MAKE) requirements

requirements: ## Install/refresh all Python project requirements
	@echo "--- Installing dependencies using uv... ---"
	@unset CONDA_PREFIX \
	&& $(PYTHON) -m pip install --upgrade pip uv \
	&& $(UV) sync --python $(PYTHON) --frozen --all-groups --all-extras --no-install-workspace --compile-bytecode --no-build --no-install-package greenify \
	&& $(UV) pip install --python $(PYTHON) --compile-bytecode greenify  # greenify lacks wheels, so it's installed separately

stub-gen: .venv ## Generate Python type stubs
	@echo "--- Generating Python type stubs... ---"
	@just generate python
	@cargo run --bin stub_gen $(ARGS)
	@-$(RUFF) check python/**/*.pyi --fix --unsafe-fixes --quiet
	@$(MAKE) fmt

build-dev: stub-gen ## Compile and produce a development wheel
	@echo "--- Building development wheel... ---"
	@unset CONDA_PREFIX \
	&& $(MATURIN) build -m ./Cargo.toml $(ARGS)

build-release: stub-gen ## Compile and produce a release wheel with optimizations
	@echo "--- Building release wheel... ---"
	@unset CONDA_PREFIX \
	&& $(MATURIN) build -m ./Cargo.toml --profile release --strip --release $(ARGS)

install-dev: stub-gen ## Build and install the development wheel in the current venv
	@echo "--- Installing the built wheel... ---"
	@unset CONDA_PREFIX \
	&& $(MATURIN) develop -m ./Cargo.toml $(ARGS)

install-release: .venv ## Build and install the release wheel in the current venv
	@echo "--- Installing the built release wheel... ---"
	@unset CONDA_PREFIX \
	&& $(MATURIN) develop -m ./Cargo.toml --profile release --strip --release $(ARGS)

fix: .venv ## Auto-fix linting issues in Rust and Python
	@echo "--- Applying automatic fixes... ---"
	cargo clippy --workspace --all-targets --all-features --fix
	$(RUFF) check --fix
	@$(MAKE) fmt

lint: .venv ## Run all lint checks for Rust and Python
	@echo "--- Running Clippy checks... ---"
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings -D clippy::dbg_macro
	@echo "--- Running Ruff checks... ---"
	$(RUFF) check
	@echo "--- Running misc checks... ---"
	$(VENV_BIN)/mbake format --check Makefile
	$(VENV_BIN)/mbake validate Makefile
	$(VENV_BIN)/dprint check

fmt: .venv ## Format all code (Rust, Python, etc.)
	@echo "--- Formatting Rust... ---"
	@cargo fmt --all
	@echo "--- Formatting Python... ---"
	@$(RUFF) format --quiet
	@echo "--- Formatting Misc... ---"
	@$(VENV_BIN)/dprint fmt --allow-no-files --log-level silent
	@$(VENV_BIN)/mbake format Makefile

pre-commit: fmt lint ## Run all code formatting and quality checks

clean: ## Clean up all caches, build artifacts, and the venv
	@echo "--- Cleaning ruff cache... ---"
	@-$(RUFF) clean --quiet
	@echo "--- Cleaning cargo... ---"
	@-cargo clean --quiet
	@echo "--- Cleaning $(VENV) build/ dist/ .pytest_cache/ .mypy_cache/ .hypothesis/ ---"
	@-rm -rf $(VENV) build/ dist/
	@-rm -rf .pytest_cache/ .mypy_cache/ .hypothesis/
	@echo "--- Cleaning *.py[co] **/__pycache__ ... ---"
	@-find . -type f -name '*.py[co]' -delete -or -type d -name __pycache__ -or -type f -name '_opendal*.so' -exec rm -rf {} +

help: ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort