# Huong Dan Chay Thu Va Test Movie Recommendation Platform

File nay huong dan chay thu du an tren Windows PowerShell, tu thu muc goc repo:

```powershell
cd "D:\School\Big Data\data analyst-recom\projectBd"
```

## 1. Yeu Cau

Can co san:

- Docker Desktop dang chay
- Python virtualenv `.venv` trong repo
- Du lieu MovieLens trong `data/archive/`
- File env Docker: `infra/docker/.env`

Kiem tra nhanh:

```powershell
docker --version
.\.venv\Scripts\python.exe --version
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env config
```

## 2. Cai Dependency Python

Chay lenh nay khi moi clone, sau khi sua `backend/requirements.txt`, hoac khi test bao thieu package:

```powershell
.\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
```

## 3. Chay Ha Tang Docker

Khoi dong toan bo service:

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env up -d --build
```

Kiem tra container:

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env ps
```

Kiem tra healthcheck:

```powershell
powershell -ExecutionPolicy Bypass -File infra\scripts\healthcheck.ps1
```

Neu thanh cong se thay:

```text
API health check passed: http://localhost:8000/health
All health checks passed.
```

## 4. Tao Artifact Recommendation

### Cach nhanh de test

Lenh nay build artifact nho tu `data/archive`, chay nhanh de test API:

```powershell
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 build-artifacts --max-movies 10 --max-ratings 100 --max-tags 10 --svd-dim 2 --max-features 100 --item-k 3 --user-k 2
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 generate-candidates --max-users 20 --max-ratings 100
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 evaluate --k 5 --max-ratings 100 --max-movies 10
```

Ket qua nam o:

```text
data\models\phase04\
```

### Cach full data

Neu muon build tren du lieu lon, bo cac tham so `--max-*`:

```powershell
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 build-artifacts
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 generate-candidates --max-users 2000
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.run_phase4 evaluate --k 10
```

## 5. Tao Index Elasticsearch

### Index sample de test nhanh

```powershell
$env:FORCE_INDEX='true'
$env:SAMPLE_INDEX='true'
.\.venv\Scripts\python.exe -B -m backend.app.infrastructure.elasticsearch.indexer
```

### Index full movies

```powershell
$env:FORCE_INDEX='true'
Remove-Item Env:\SAMPLE_INDEX -ErrorAction SilentlyContinue
.\.venv\Scripts\python.exe -B -m backend.app.infrastructure.elasticsearch.indexer
```

Kiem tra index:

```powershell
Invoke-WebRequest -Uri http://127.0.0.1:9200/_cat/indices?v -UseBasicParsing
```

## 6. Test API

Health:

```powershell
Invoke-WebRequest -Uri http://127.0.0.1:8000/health -UseBasicParsing
```

Search:

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8000/movies/search?q=Toy&page=1&size=3" -UseBasicParsing
```

Recommendation:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/recommendations/1?k=5" | ConvertTo-Json -Depth 5
```

Track event hop le:

```powershell
$body = '{"user_id":1,"movie_id":1,"event_type":"view"}'
Invoke-WebRequest -Uri http://127.0.0.1:8000/events/track -Method POST -ContentType application/json -Body $body -UseBasicParsing
```

Track event rate thieu rating, ky vong loi `422`:

```powershell
$body = '{"user_id":1,"movie_id":1,"event_type":"rate"}'
try {
  Invoke-WebRequest -Uri http://127.0.0.1:8000/events/track -Method POST -ContentType application/json -Body $body -UseBasicParsing
} catch {
  $_.Exception.Response.StatusCode.value__
}
```

## 7. Test Consumer Kafka -> Redis

Sau khi gui it nhat 1 event bang `/events/track`, chay consumer mot event:

```powershell
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase05.streaming_consumer --bootstrap-servers localhost:9094 --redis-host localhost --redis-port 6379 --max-events 1
```

Ky vong log cuoi co:

```text
events_processed: 1
recommendations_updated: 1
errors: 0
```

Kiem tra recommendation da doc tu cache:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/recommendations/1?k=2" | ConvertTo-Json -Depth 5
```

Neu `"cached": true` la duong Kafka -> Redis -> API da hoat dong.

## 8. Chay Smoke Tests

Phase 4:

```powershell
.\.venv\Scripts\python.exe -B -m backend.app.application.use_cases.phase04.smoke_test
```

Phase 5:

```powershell
.\.venv\Scripts\python.exe -B -m pytest backend\app\application\use_cases\phase05\smoke_tests.py -q -p no:cacheprovider
```

Ky vong Phase 5:

```text
8 passed
```

## 9. Xem Log Va Dung Service

Xem log API:

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env logs --tail 100 api
```

Xem log Kafka:

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env logs --tail 100 kafka
```

Dung toan bo service:

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env down
```

## 10. Loi Thuong Gap

- `No module named backend`: hay chay API tu thu muc goc bang `backend.main:app`, khong chay `main:app` trong thu muc `backend`.
- Search tra rong: chua tao Elasticsearch index, chay lai buoc 5.
- Recommendation rong: chua tao `data/models/phase04`, chay lai buoc 4.
- Event track loi 500: kiem tra Kafka co healthy khong bang `docker compose ... ps`.
- TestClient loi voi `httpx`: dam bao `backend/requirements.txt` da cai `httpx>=0.27.0,<0.28.0`.
- Elasticsearch loi compatible header: dam bao client Python la `elasticsearch>=8.0.0,<9.0.0`.
