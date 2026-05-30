# Local Development

## Prerequisites
- Docker Desktop (Compose v2 enabled)
- PowerShell 5.1+ (for the healthcheck script on Windows)

## Setup
1. Copy the env template:
   - `copy infra\docker\.env.example infra\docker\.env`
2. Update ports or credentials in `infra/docker/.env` if needed.

## Start Services
From the repo root:
- `docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env up -d`

Or from the infra directory:
- `cd infra/docker`
- `docker compose --env-file .env up -d`

## Verify Health
Run the healthcheck script:
- `powershell -ExecutionPolicy Bypass -File infra/scripts/healthcheck.ps1`

Expected:
- All containers report healthy
- `GET /health` returns 200 from the API container

## Stop Services
- `docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env down`

## Reset Volumes (Destructive)
- `docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env down -v`

## Default Endpoints
- API: `http://localhost:8000/health`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Elasticsearch: `http://localhost:9200`
- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- Kafka: `localhost:9094`
- MySQL: `localhost:3306`
- Redis: `localhost:6379`

## Troubleshooting
- Port conflicts: change host ports in `infra/docker/.env` and restart.
- Elasticsearch fails to start: reduce `ELASTICSEARCH_JAVA_OPTS` heap size.
- Kafka not healthy: confirm Docker has enough memory and retry `docker compose up -d`.
