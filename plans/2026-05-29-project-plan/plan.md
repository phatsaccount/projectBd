---
title: "Movie Recommendation Platform Plan"
description: "Lean phased roadmap for a demo-grade end-to-end system."
status: pending
priority: P2
effort: 120h
branch: main
tags: [plan, roadmap, backend, data, infra]
created: 2026-05-29
---

# Overview
This plan delivers a lean, end-to-end demo: ingestion -> batch ETL -> search -> model -> streaming events -> recommendation serving. It is optimized for a university capstone scope while keeping the architecture clean.

# Assumptions
- The current repo structure and core components (API, batch jobs, streaming jobs, infra) remain in place.
- Development is local-first with Docker Compose for infra dependencies.
- Data sources are MovieLens CSVs and user events arriving via Kafka.
- Scope targets demo-level functionality; advanced MLOps and enterprise features are excluded.

# Scope
In scope:
- Local infra via Docker Compose
- Data ingestion and baseline schema
- Batch ETL and feature pipeline
- Search index and Search API
- Model training and offline evaluation
- Event streaming and recommendation serving (minimal, demo-grade)
- Basic tests and local runbook

Out of scope:
- UI or frontend experience
- Mobile clients
- Production multi-region deployment
- Advanced MLOps (A/B test, online learning)
- Enterprise auth/SSO
- Cost optimization/FinOps

# Data Flow (Explicit)
1. Raw CSV -> storage -> schema validation.
2. Batch ETL -> cleaned data -> feature tables -> model artifacts.
3. Search index built from cleaned data -> Search API responses.
4. User events -> Kafka -> streaming job -> cache/state -> Recommendation API responses.

# Dependency Graph
- Phase 0 blocks all other phases.
- Phase 1 blocks Phase 2 and Phase 3.
- Phase 2 blocks Phase 3 and Phase 4.
- Phase 3 blocks Phase 5.
- Phase 4 blocks Phase 5.

# Phases
## Phase 0: Foundations and Environment (10h)
Dependencies: none
Ownership: Infra/DevOps
Deliverables:
- Docker Compose baseline with pinned versions
- Env template for local development
- Health checks and local runbook
Success criteria:
- One-command local boot brings services up
- Health endpoints respond without manual fixes
- Local runbook validated from a clean clone
Risks (L/M/H):
- Version drift across services (M) -> pin versions and document
- Port conflicts on developer machines (M) -> provide defaults and overrides
Tests:
- Infra smoke tests (container health)
Rollback:
- Revert to last known working compose config

## Phase 1: Data Ingestion and Baseline Schema (14h)
Dependencies: Phase 0
Ownership: Data Engineering
Deliverables:
- Raw data ingest to storage
- Baseline schemas documented and validated
- Basic data sanity checks (missing ids, duplicates)
Success criteria:
- Raw datasets load consistently end-to-end
- Schema is stable and usable by Spark jobs
- Bad rows are handled or reported
Risks (L/M/H):
- Schema mismatches (M) -> strict parsing with fallback rules
Tests:
- Unit tests for parsers
- Sample ingest run on a small dataset
Rollback:
- Keep prior raw data snapshot and revert to last schema

## Phase 2: Batch ETL and Feature Pipeline (18h)
Dependencies: Phase 1
Ownership: Data Engineering
Deliverables:
- Cleaned datasets and feature tables
- Reproducible batch job scripts
Success criteria:
- Batch jobs rerun with consistent outputs
- Feature tables are usable for training and indexing
Risks (L/M/H):
- Data quality regressions (M) -> add validation checkpoints
Tests:
- Unit tests for transformations
- Integration run on sample data
Rollback:
- Retain previous cleaned/features output and revert

## Phase 3: Search Index and API Skeleton (18h)
Dependencies: Phase 1, Phase 2
Ownership: Backend
Deliverables:
- ES mapping and index build script
- Search API and movie detail endpoint
- Basic request/response schemas
Success criteria:
- Search returns relevant results on sample queries
- API handles empty or missing cases gracefully
Risks (L/M/H):
- Mapping mismatch (M) -> iterate mapping with small dataset
- Query relevance issues (M) -> tune fields and analyzers
Tests:
- Integration test with sample index
- API contract tests for core endpoints
Rollback:
- Revert index mapping and routes to last known working state

## Phase 4: Model Training and Offline Evaluation (20h)
Dependencies: Phase 2
Ownership: ML Engineering
Deliverables:
- Baseline recommendation model artifact
- Offline evaluation summary (qualitative)
Success criteria:
- Training runs end-to-end without manual fixes
- Evaluation summary is recorded and repeatable
- Model artifacts are stored and versioned
Risks (L/M/H):
- Weak baseline quality (M) -> iterate features or algorithm choice
Tests:
- Unit tests for training utilities
- Reproducibility check on small subset
Rollback:
- Keep previous model artifacts and evaluation notes

## Phase 5: Streaming Events and Recommendation Serving (20h)
Dependencies: Phase 3, Phase 4
Ownership: Backend + Data Engineering
Deliverables:
- Event ingestion API
- Kafka consumer/streaming job
- Cache or state updates for recommendations
- Recommendation API (uses cache/model output)
Success criteria:
- Demo shows events can change recommendation output
- System continues to serve baseline results if streaming is off
Risks (L/M/H):
- Event duplication (M) -> idempotent processing
- Cache inconsistency (M) -> clear refresh strategy
Tests:
- Integration tests with local Kafka
- End-to-end demo script for event -> recommendation
Rollback:
- Disable streaming and serve static model results

# Backwards Compatibility Strategy
- Version data schemas and keep N-1 compatibility where feasible.
- Keep previous model artifacts and index mappings for rollback.
- Maintain stable API responses for demo clients.

# Test Matrix
- Unit: parsers, transformations, model utilities
- Integration: storage connectors, search index, Kafka consumer
- E2E: search flow, recommend flow with events

# Success Criteria (Global)
- End-to-end demo runs from data -> search -> recommend.
- Local boot is reliable with a single command.
- Core APIs return consistent responses on sample data.

# Open Questions
- Target dataset size for the demo run?
- Minimum acceptable recommendation quality (described in words)?
- Do we need a scripted demo scenario for presentation?
