





# Tổng Quan Dự Án Movie Recommendation Platform

Tài liệu này tóm tắt dự án và cung cấp hướng dẫn chạy cục bộ cho môi trường hiện tại của repo.

## 1. Dự án này làm gì

Đây là một nền tảng tìm kiếm và gợi ý phim, lấy cảm hứng từ MovieLens, với các thành phần chính:

- API backend bằng FastAPI
- tìm kiếm phim bằng Elasticsearch
- gợi ý phim theo dữ liệu Phase 4 và cache Redis
- ghi nhận sự kiện người dùng qua Kafka
- xử lý dữ liệu bằng PySpark
- triển khai hạ tầng bằng Docker Compose

Mục tiêu của hệ thống là:

- cho phép tra cứu phim nhanh
- nhận event người dùng như xem, đánh giá, bookmark
- cập nhật và phục vụ recommendation theo thời gian gần thực
- chuẩn bị dữ liệu offline cho mô hình / artifact gợi ý

## 2. Kiến trúc chính

Dự án đang đi theo hướng Clean Architecture:

- `backend/`: API và business use case
- `spark-jobs/`: job batch và streaming với PySpark
- `infra/`: Docker Compose, script healthcheck, script setup
- `data/`: dữ liệu raw, cleaned, features, models
- `docs/`: tài liệu kiến trúc và runbook

Trong backend, các phần đáng chú ý là:

- `app/interfaces/api/routes/`: các route HTTP
- `app/application/use_cases/`: logic use case
- `app/infrastructure/`: kết nối Kafka, Redis, Elasticsearch
- `main.py`: điểm khởi động FastAPI

## 3. Các chức năng hiện có

API hiện có các endpoint chính:

- `GET /health`: kiểm tra trạng thái API
- `GET /movies/search`: tìm phim theo từ khóa
- `GET /movies/{movie_id}`: lấy chi tiết một phim
- `POST /events/track`: ghi nhận event người dùng và đẩy sang Kafka
- `GET /recommendations/{user_id}`: lấy danh sách phim gợi ý

Luồng recommendation hiện tại có 2 lớp:

- cache Redis cho dữ liệu đã tính sẵn
- fallback từ artifact Phase 4 nếu cache chưa có dữ liệu

## 4. Yêu cầu trước khi chạy

Bạn cần có:

- Docker Desktop với Docker Compose
- Python 3.8+ hoặc mới hơn
- dữ liệu đầu vào ở `data/raw/` cho Phase 4; nếu chưa có, code sẽ tự dùng `data/archive/` khi đủ `movie.csv`, `rating.csv`, `tag.csv`
- dependencies Python trong `backend/requirements.txt`

Nếu chạy đầy đủ luồng gợi ý, hãy bảo đảm Phase 4 artifacts đã được tạo trong `data/models/phase04/`.

## 5. Cách chạy dự án cục bộ

### Bước 1: Tạo file môi trường

Từ thư mục gốc của repo:

```powershell
copy infra\docker\.env.example infra\docker\.env
```

Nếu cần, chỉnh port hoặc mật khẩu trong `infra/docker\.env`.

### Bước 2: Khởi động hạ tầng

```powershell
docker compose -f infra/docker/docker-compose.yml --env-file infra/docker/.env up -d
```

Hạ tầng này bao gồm:

- MySQL
- Redis
- Elasticsearch
- Kafka + Zookeeper
- MinIO
- Spark master / worker

### Bước 3: Kiểm tra trạng thái

```powershell
powershell -ExecutionPolicy Bypass -File infra/scripts/healthcheck.ps1
```

Bạn cũng có thể kiểm tra nhanh:

```powershell
curl http://localhost:8000/health
```

### Bước 4: Cài dependencies cho backend

```powershell
cd backend
pip install -r requirements.txt
```

### Bước 5: Tạo Phase 4 artifacts

Nếu bạn muốn recommendation fallback hoạt động đầy đủ, chạy build artifacts trước:

```powershell
python -m backend.app.application.use_cases.phase04.run_phase4 build-artifacts
python -m backend.app.application.use_cases.phase04.run_phase4 generate-candidates --max-users 2000
python -m backend.app.application.use_cases.phase04.run_phase4 evaluate --k 10
```

Kết quả sẽ nằm trong `data/models/phase04/`.

### Bước 6: Chạy API

Từ thư mục gốc của repo:

```powershell
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

### Bước 7: Chạy streaming consumer

Từ thư mục gốc của repo:

```powershell
python -m backend.app.application.use_cases.phase05.streaming_consumer --bootstrap-servers localhost:9094 --redis-host localhost --redis-port 6379
```

Nếu môi trường Kafka của bạn được map khác cổng, hãy dùng đúng giá trị đó cho `--bootstrap-servers`.

### Bước 8: Chạy demo end-to-end

Từ thư mục gốc của repo:

```powershell
python -m backend.app.application.use_cases.phase05.demo --api-url http://localhost:8000 --user-id 1 --num-events 8
```

Demo này sẽ:

- kiểm tra API
- lấy recommendation ban đầu
- gửi một loạt event
- chờ consumer xử lý
- lấy recommendation sau cùng để so sánh

## 6. Gợi ý kiểm thử nhanh

- `GET http://localhost:8000/health`
- `GET http://localhost:8000/movies/search?q=avatar`
- `POST http://localhost:8000/events/track`
- `GET http://localhost:8000/recommendations/1?k=5`

Ví dụ `curl`:

```powershell
curl "http://localhost:8000/movies/search?q=avatar&page=1&size=10"
curl http://localhost:8000/recommendations/1?k=5
```

## 7. Lưu ý khi chạy

- Nếu recommendation trả về rỗng, hãy kiểm tra `data/models/phase04/` đã có artifact chưa.
- Nếu event không đi qua Kafka, kiểm tra broker đang chạy và cổng bootstrap đúng với môi trường của bạn.
- Nếu Redis hoặc Elasticsearch không khởi động, xem lại tài nguyên Docker và port đang bị chiếm.
- Hãy ưu tiên chạy hạ tầng trước, rồi mới chạy API, consumer, và demo.

## 8. Tài liệu liên quan

- [docs/architecture.md](architecture.md)
- [docs/phase-05-runbook.md](phase-05-runbook.md)
- [docs/ai_context.md](ai_context.md)
- [backend/app/shared/utils/local-dev.md](../backend/app/shared/utils/local-dev.md)
