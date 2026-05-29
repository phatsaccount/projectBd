# Project Codex — Movie Recommendation Platform

This repository implements a movie search and recommendation platform based on MovieLens data. It combines a Clean Architecture backend, batch/streaming data pipelines in PySpark, and infrastructure services for search, cache, storage, and event streaming.

## Architecture Summary
- Backend follows Clean Architecture: domain (pure business logic), application (use cases), infrastructure (DB/ES/Kafka/Redis/MinIO adapters), interfaces (API routes, schemas, presenters), shared (utils, logging, constants, exceptions).
- Data processing is split into batch jobs (ETL, feature building, model training) and streaming jobs (Kafka consumption, profile updates, realtime recommendations).
- Deploy is based on Docker Compose for the full service stack.

## Repository Map (Key Paths)
- backend/ : FastAPI app, Clean Architecture layers
- spark-jobs/ : PySpark batch and streaming jobs
- data/ : raw inputs, cleaned data, features, trained models
- infra/ : Docker Compose and setup scripts
- docs/ : architecture, API, data flow documentation

## Core Data Flow (High-Level)
1. Raw MovieLens CSV data is ingested into data/raw or object storage.
2. Batch Spark jobs clean data, build features, and train recommenders.
3. Streaming Spark jobs consume Kafka events to update user profiles and caches.
4. Backend API serves search and recommendations, prioritizing Redis cache and Elasticsearch.

## Services and Dependencies
- Elasticsearch: search index and query
- MySQL: persistent relational storage
- Redis: cache and fast lookup
- Kafka: event streaming
- MinIO: object storage for datasets and artifacts
- Spark: batch/stream processing and model training

## Environment Variables (Typical)
- DATABASE_URL / MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD
- ES_HOST / ES_PORT
- REDIS_URL
- KAFKA_BOOTSTRAP_SERVERS
- MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY

## Development Notes for AI Assistance
- Keep domain layer pure: avoid framework imports in domain.
- Use cases should call interfaces/ports, not concrete adapters.
- Infrastructure implements repository interfaces and external IO.
- API routes should be thin: validate, call use case, return response.
- Reuse shared helpers and schemas from shared/ and spark-jobs/common/.

## Suggested Entry Points
- Backend app: backend/main.py
- Docker Compose: infra/docker/docker-compose.yml
- Batch jobs: spark-jobs/batch/*.py
- Streaming jobs: spark-jobs/streaming/*.py

## Example Features to Implement
- Add or refine SearchMovieUseCase with ES query tuning.
- Add Redis caching layer for recommendations.
- Implement new Kafka event consumer for user events.
- Add ETL steps to enrich movie metadata.

