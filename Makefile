# ============================================
# NYC Taxi Pipeline - Makefile
# ============================================
# Run 'make help' to see available commands
# ============================================

.PHONY: help setup install install-dev clean \
        start stop restart logs status \
        test test-unit test-integration test-cov \
        lint format type-check quality \
        terraform-init terraform-validate terraform-plan terraform-apply \
        docker-build docker-push \
        download-data run-bronze run-transform run-full \
        db-shell spark-shell demo

# ============================================
# Variables
# ============================================
DOCKER_COMPOSE = docker-compose -f docker/docker-compose.yml
PYTHON = python
PIP = pip
PYTEST = pytest
BLACK = black
ISORT = isort
FLAKE8 = flake8
MYPY = mypy

# Default months to process
MONTHS ?= 2023-01

# Colors for terminal output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

# ============================================
# Help
# ============================================
help: ## Show this help message
	@echo "$(BLUE)NYC Taxi Pipeline - Available Commands$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Examples:$(RESET)"
	@echo "  make setup              # Initial project setup"
	@echo "  make start              # Start Docker services"
	@echo "  make run-full MONTHS=2023-01 2023-02"
	@echo "  make test               # Run all tests"

# ============================================
# Setup & Installation
# ============================================
setup: ## Initial project setup (create dirs, install deps)
	@echo "$(BLUE)Setting up project...$(RESET)"
	@mkdir -p data/{raw,bronze,silver,gold}
	@touch data/raw/.gitkeep data/bronze/.gitkeep data/silver/.gitkeep data/gold/.gitkeep
	@cp -n .env.example .env 2>/dev/null || true
	@cp -n docker/.env.example docker/.env 2>/dev/null || true
	@$(PIP) install -e ".[dev]"
	@echo "$(GREEN)Setup complete!$(RESET)"

install: ## Install production dependencies
	@$(PIP) install -e .

install-dev: ## Install development dependencies
	@$(PIP) install -e ".[dev]"

install-all: ## Install all dependencies (including GCP)
	@$(PIP) install -e ".[all]"

clean: ## Clean temporary files and caches
	@echo "$(BLUE)Cleaning...$(RESET)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf build/ dist/ htmlcov/ .coverage coverage.xml
	@echo "$(GREEN)Clean complete!$(RESET)"

clean-data: ## Clean all data directories (CAUTION!)
	@echo "$(RED)Warning: This will delete all data files!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	@rm -rf data/raw/* data/bronze/* data/silver/* data/gold/*
	@touch data/raw/.gitkeep data/bronze/.gitkeep data/silver/.gitkeep data/gold/.gitkeep
	@echo "$(GREEN)Data cleaned!$(RESET)"

# ============================================
# Docker Services
# ============================================
start: ## Start Docker services (Spark + PostgreSQL)
	@echo "$(BLUE)Starting services...$(RESET)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(YELLOW)Waiting for services to be ready...$(RESET)"
	@sleep 10
	@echo "$(GREEN)Services started!$(RESET)"
	@echo "  Spark UI:  http://localhost:8081"
	@echo "  Adminer:   http://localhost:8080"
	@echo "  PostgreSQL: localhost:5432"

stop: ## Stop Docker services
	@echo "$(BLUE)Stopping services...$(RESET)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)Services stopped!$(RESET)"

restart: stop start ## Restart Docker services

logs: ## Show Docker logs (follow mode)
	@$(DOCKER_COMPOSE) logs -f

logs-spark: ## Show Spark logs
	@$(DOCKER_COMPOSE) logs -f spark-master spark-worker

logs-postgres: ## Show PostgreSQL logs
	@$(DOCKER_COMPOSE) logs -f postgres

status: ## Show status of Docker services
	@$(DOCKER_COMPOSE) ps

destroy: ## Stop and remove all containers and volumes (CAUTION!)
	@echo "$(RED)Warning: This will destroy all containers and volumes!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@echo "$(GREEN)Destroyed!$(RESET)"

# ============================================
# Testing
# ============================================
test: ## Run all tests
	@echo "$(BLUE)Running all tests...$(RESET)"
	@$(PYTEST) tests/ -v

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(RESET)"
	@$(PYTEST) tests/unit/ -v -m "not slow"

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(RESET)"
	@$(PYTEST) tests/integration/ -v

test-cov: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(RESET)"
	@$(PYTEST) tests/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html \
		--cov-report=xml \
		-v
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(RESET)"

test-fast: ## Run fast tests only (skip slow)
	@$(PYTEST) tests/ -v -m "not slow and not integration"

# ============================================
# Code Quality
# ============================================
lint: ## Run linter (flake8)
	@echo "$(BLUE)Running linter...$(RESET)"
	@$(FLAKE8) src/ tests/

format: ## Format code (black + isort)
	@echo "$(BLUE)Formatting code...$(RESET)"
	@$(BLACK) src/ tests/
	@$(ISORT) src/ tests/
	@echo "$(GREEN)Formatting complete!$(RESET)"

format-check: ## Check code formatting without changes
	@echo "$(BLUE)Checking format...$(RESET)"
	@$(BLACK) --check src/ tests/
	@$(ISORT) --check-only src/ tests/

type-check: ## Run type checker (mypy)
	@echo "$(BLUE)Running type checker...$(RESET)"
	@$(MYPY) src/

quality: lint format-check type-check ## Run all quality checks
	@echo "$(GREEN)All quality checks passed!$(RESET)"

# ============================================
# Terraform
# ============================================
terraform-init: ## Initialize Terraform
	@echo "$(BLUE)Initializing Terraform...$(RESET)"
	@cd terraform && terraform init

terraform-validate: ## Validate Terraform configuration
	@echo "$(BLUE)Validating Terraform...$(RESET)"
	@cd terraform && terraform fmt -check -recursive
	@cd terraform && terraform validate
	@echo "$(GREEN)Terraform validation passed!$(RESET)"

terraform-fmt: ## Format Terraform files
	@echo "$(BLUE)Formatting Terraform files...$(RESET)"
	@cd terraform && terraform fmt -recursive
	@echo "$(GREEN)Terraform formatting complete!$(RESET)"

terraform-plan: ## Plan Terraform changes
	@echo "$(BLUE)Planning Terraform changes...$(RESET)"
	@cd terraform && terraform plan

terraform-apply: ## Apply Terraform changes (CAUTION!)
	@echo "$(RED)Warning: This will apply changes to GCP!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	@cd terraform && terraform apply

terraform-destroy: ## Destroy Terraform resources (CAUTION!)
	@echo "$(RED)Warning: This will DESTROY all GCP resources!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	@cd terraform && terraform destroy

# ============================================
# Docker Build
# ============================================
docker-build: ## Build Docker image
	@echo "$(BLUE)Building Docker image...$(RESET)"
	@docker build -f docker/Dockerfile -t nyc-taxi-pipeline:latest .
	@echo "$(GREEN)Docker image built!$(RESET)"

docker-build-no-cache: ## Build Docker image without cache
	@docker build -f docker/Dockerfile -t nyc-taxi-pipeline:latest --no-cache .

# ============================================
# Pipeline Execution
# ============================================
download-data: ## Download NYC Taxi data
	@echo "$(BLUE)Downloading data for months: $(MONTHS)$(RESET)"
	@./scripts/download_data.sh $(MONTHS)
	@echo "$(GREEN)Download complete!$(RESET)"

run-bronze: ## Run bronze ingestion job
	@echo "$(BLUE)Running bronze ingestion for: $(MONTHS)$(RESET)"
	@$(DOCKER_COMPOSE) run --rm pipeline python -m src.cli bronze --months $(MONTHS)

run-transform: ## Run dimensional transformation job
	@echo "$(BLUE)Running transformation for: $(MONTHS)$(RESET)"
	@$(DOCKER_COMPOSE) run --rm pipeline python -m src.cli transform --months $(MONTHS)

run-full: ## Run full pipeline (bronze + transform)
	@echo "$(BLUE)Running full pipeline for: $(MONTHS)$(RESET)"
	@$(DOCKER_COMPOSE) run --rm pipeline python -m src.cli full --months $(MONTHS)

run-backfill: ## Backfill specific months
	@echo "$(BLUE)Backfilling months: $(MONTHS)$(RESET)"
	@$(DOCKER_COMPOSE) run --rm pipeline python -m src.cli bronze --months $(MONTHS) --mode overwrite
	@$(DOCKER_COMPOSE) run --rm pipeline python -m src.cli transform --months $(MONTHS)

# ============================================
# Interactive Shells
# ============================================
db-shell: ## Open PostgreSQL shell
	@$(DOCKER_COMPOSE) exec postgres psql -U pipeline -d nyctaxi

spark-shell: ## Open PySpark shell
	@$(DOCKER_COMPOSE) exec spark-master /opt/bitnami/spark/bin/pyspark

bash-shell: ## Open bash shell in pipeline container
	@$(DOCKER_COMPOSE) run --rm pipeline bash

# ============================================
# Demo & Shortcuts
# ============================================
demo: start download-data run-full ## Full demo: start services, download data, run pipeline
	@echo "$(GREEN)Demo complete!$(RESET)"
	@echo "$(YELLOW)Check results:$(RESET)"
	@echo "  - Adminer: http://localhost:8080 (System: PostgreSQL, Server: postgres, User: pipeline, Password: pipeline123, Database: nyctaxi)"
	@echo "  - Spark UI: http://localhost:8081"

ci-local: quality test terraform-validate docker-build ## Run full CI pipeline locally
	@echo "$(GREEN)Local CI passed!$(RESET)"

# ============================================
# Monitoring
# ============================================
monitor: ## Show pipeline health report (requires Docker running)
	@echo "$(BLUE)Running pipeline monitor...$(RESET)"
	@$(DOCKER_COMPOSE) --profile jobs run --rm pipeline python scripts/monitor.py

monitor-local: ## Show pipeline health report (local, requires POSTGRES_HOST=localhost)
	@echo "$(BLUE)Running pipeline monitor locally...$(RESET)"
	@POSTGRES_HOST=localhost $(PYTHON) scripts/monitor.py

monitor-demo: ## Demo the PipelineMonitor class (no DB required)
	@echo "$(BLUE)Running monitor demo...$(RESET)"
	@$(PYTHON) scripts/monitor.py --demo

# ============================================
# Documentation
# ============================================
docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving docs at http://localhost:8000$(RESET)"
	@cd docs && python -m http.server 8000
