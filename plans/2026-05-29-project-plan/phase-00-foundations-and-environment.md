---
phase: 0
title: "Foundations and Environment"
status: pending
priority: P1
effort: "12h"
dependencies: []
---

# Phase 0: Foundations and Environment

## Overview
Establish a reproducible local environment and configuration baseline for all services. This phase delivers a stable, one-command boot with documented configuration and health checks.

## Requirements
- Functional:
  - One-command local boot for all core services.
  - Environment variable template with required values and defaults.
  - Health checks for API and key dependencies.
- Non-functional:
  - Reproducible versions pinned in compose and images.
  - Minimal secret handling for local development.

## Architecture
- Config flow: env template -> docker compose -> service containers -> health endpoints.
- Service readiness: container health -> API /health -> smoke verification script.

## Related Code Files
- Modify:
  - infra/docker/docker-compose.yml
  - infra/docker/** (service-specific configs if needed)
- Create:
  - infra/docker/.env.example
  - infra/scripts/healthcheck.ps1 (or .sh) for local verification
  - docs/local-dev.md (local setup and troubleshooting)
- Delete:
  - None

## Implementation Steps
1. Audit current compose setup and pin versions for all services.
2. Define required env vars and create .env.example with safe defaults.
3. Add or confirm health checks for API, database, cache, and search.
4. Create a local healthcheck script that verifies container health and API readiness.
5. Document the local runbook and troubleshooting steps.
6. Validate a clean boot from a fresh clone.

## Success Criteria
- `docker compose -f infra/docker/docker-compose.yml up -d` brings all services to healthy state within 5 minutes.
- `GET /health` returns 200 after boot.
- All required env vars are documented in .env.example.
- Local runbook provides a clear start/stop workflow and basic troubleshooting.

## Risk Assessment
- Version drift across services (M) -> Pin versions and record in compose.
- Port conflicts on developer machines (M) -> Provide configurable ports and defaults.
- Missing env vars causing boot failures (M) -> Validate in healthcheck script.

## Rollback Plan
- Revert compose changes to last known working version.
- Restore previous env template if new variables cause breakage.
