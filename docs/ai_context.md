# Tóm tắt dự án — Movie Recommendation Platform (Context cho AI)

Mục đích: cung cấp ngữ cảnh ngắn gọn, đủ để một AI hiểu cấu trúc, luồng dữ liệu, công nghệ chính, và điểm chạm quan trọng trong repo này, nhằm hỗ trợ phát triển, review, hoặc tự động hoá các tác vụ kỹ thuật.

## Tổng quan
- Tên dự án: Hệ thống tìm kiếm & gợi ý phim (dựa trên MovieLens)
- Mục tiêu: indexing & search (Elasticsearch), real-time/user-based recommendations (Spark + model), ingestion/event streaming (Kafka), lưu trữ tài liệu/asset (MinIO), persistence (MySQL), cache/session (Redis), deploy bằng Docker.

## Cấu trúc chính (vị trí quan trọng)
- `backend/` – API (FastAPI), chia theo Clean Architecture: `domain/`, `application/`, `infrastructure/`, `interfaces/`, `shared/`.
- `spark-jobs/` – jobs PySpark: `batch/` (ETL, train), `streaming/` (consume Kafka, realtime updates), `common/`.
- `data/` – `raw/`, `cleaned/`, `features/`, `models/`.
- `infra/` – Docker Compose, scripts init (MySQL, ES indices, seed data).
- `docs/` – tài liệu (kiến trúc, API, data flow).

## Công nghệ & thư viện chính
- Python 3.x, FastAPI (backend)
- PySpark (data processing)
- Elasticsearch (search/index)
- MySQL (persistent store)
- Redis (cache)
- Kafka (event streaming)
- MinIO (object storage)
- Docker / Docker Compose

## Luồng dữ liệu tóm tắt
1. Dữ liệu thô (MovieLens CSV) lưu ở `data/raw/` hoặc MinIO.
2. Batch ETL (spark-jobs/batch) -> `data/cleaned/`, build features -> `data/features/` và train model -> `data/models/`.
3. Streaming: events người dùng vào Kafka -> spark-jobs/streaming cập nhật user profile, cache Redis, và/hoặc push cập nhật vào ES hoặc result tables.
4. Backend API: đọc cache Redis trước, nếu miss thì query Elasticsearch hoặc MySQL/model outputs.

## Entrypoints & scripts quan trọng
- Backend: `backend/main.py` (FastAPI app). Thông thường chạy bằng `uvicorn` hoặc qua Docker Compose.
- Docker Compose: `infra/docker/docker-compose.yml` (start Elasticsearch, Kafka, MySQL, Redis, MinIO, backend).
- Spark batch scripts: `spark-jobs/batch/*.py` (extract_movies.py, transform_movies.py, build_features.py, train_recommender.py).
- Spark streaming scripts: `spark-jobs/streaming/*.py` (consume_kafka_events.py, update_user_profile.py, realtime_recommendation.py).

## Biến môi trường quan trọng (ví dụ)
- `DATABASE_URL` / `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD`
- `ES_HOST` / `ES_PORT`
- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`

## Lệnh khởi động nhanh (gợi ý)
```bash
# ở root repo
docker compose -f infra/docker/docker-compose.yml up --build -d

# chạy backend cục bộ (nếu muốn dev không qua Docker)
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## API chính (kỳ vọng)
- `GET /movies/search?q=...` — tìm kiếm phim (dùng Elasticsearch)
- `GET /movies/{movie_id}` — chi tiết phim
- `GET /recommendations/{user_id}` — gợi ý cho user (cache Redis / model)
- `POST /events` — nhận event người dùng (ghi vào Kafka)
- `GET /health` — healthcheck

## Nơi lưu model & output
- Model trained: `data/models/`.
- Feature tables: `data/features/`.
- Indexes ES: cấu hình/thiết lập trong `infra/scripts/setup_indices.py` hoặc `infrastructure/elasticsearch/`.

## Nguyên tắc kiến trúc (quan trọng cho AI khi sửa/viết code)
- Domain layer phải giữ "thuần" (không import FastAPI/SQLAlchemy/ES client).
- Use cases trong `application/` điều phối hành động (cache -> repo -> format result).
- Infrastructure implement các port/repository interface.
- Interfaces chỉ làm nhiệm vụ nhận request, validate, gọi use case, trả response.

## Gợi ý cho AI khi làm việc với repo
- Trước khi thay đổi, xác định lớp (domain/application/infrastructure/interfaces).
- Khi cần truy xuất dữ liệu, implement repository trong `infrastructure/` theo interface trong `domain/repositories`.
- Tối ưu tìm kiếm: xem `infrastructure/elasticsearch/` để biết mapping và query patterns.
- Khi tạo endpoint mới, route phải gọi use case tương ứng, logic chính nằm trong `application/use_cases`.
- Khi cập nhật pipeline, kiểm tra `spark-jobs/common/` để giữ các chuẩn chung (schema, normalization).

## Nhiệm vụ AI có thể thực hiện ngay (ví dụ)
- Sinh skeleton code cho repository/adapter (ES/MySQL/Redis/MinIO).
- Viết hoặc sửa `SearchMovieUseCase` theo yêu cầu feature.
- Viết unit tests cho domain logic và use cases.
- Viết scripts để seed dữ liệu cho dev environment.
- Tạo tài liệu API hoặc sơ đồ luồng dữ liệu cho báo cáo.

---

File liên quan để tham khảo: `docs/architecture.md`, `backend/`, `spark-jobs/`, `infra/docker/docker-compose.yml`.

Nếu bạn muốn, tôi có thể: tạo skeleton code cụ thể, thêm ví dụ `requirements.txt`, hoặc sinh test cases mẫu.
