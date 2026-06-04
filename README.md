# 🎬 Movie Recommendation Platform - Quick Start Guide

Hướng dẫn chi tiết để khởi chạy dự án và demo các tính năng chính qua giao diện web.

---

## 📋 Mục lục

1. [Giới thiệu nhanh](#giới-thiệu-nhanh)
2. [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
3. [Quick Start (3 bước)](#quick-start-3-bước)
4. [Demo các tính năng](#demo-các-tính-năng)
5. [Luồng hoạt động](#luồng-hoạt-động)
6. [Troubleshooting](#troubleshooting)

---

## 🎯 Giới thiệu nhanh

Đây là một **nền tảng tìm kiếm và gợi ý phim** với các tính năng:

- 🔍 **Tìm kiếm phim** - Full-text search bằng Elasticsearch
- 💡 **Gợi ý cá nhân hóa** - Dựa trên ML models + Redis caching
- 📌 **Theo dõi sự kiện** - Ghi nhận lịch sử người dùng qua Kafka
- 📊 **Giám sát** - Grafana + Prometheus monitoring
- ⚡ **Hiệu suất cao** - API Response trong ms, cached recommendations

---

## 💻 Yêu cầu hệ thống

- **Docker Desktop** (với Docker Compose)
- **Python 3.8+** 
- **RAM tối thiểu: 8GB** (khuyến nghị 16GB)
- **Disk space: 5GB** cho images và data

**Kiểm tra:**
```bash
docker --version
python --version
```

---

## 🚀 Quick Start (3 bước)

### Bước 1️⃣: Khởi động hạ tầng (2 phút)

```bash
# Vào thư mục Docker
cd infra/docker

# Copy file environment
cp .env.example .env

# Khởi chạy tất cả services
docker-compose up -d

# Chờ các services khởi động xong
docker-compose ps
```

✅ Chờ cho đến khi tất cả containers có status `Up`

**Kiểm tra API đã sẵn sàng:**
```bash
curl http://localhost:8000/health
```

### Bước 2️⃣: Khởi động Demo UI (15 giây)

Mở **terminal mới** tại thư mục gốc:

```bash
cd demo-ui
python -m http.server 5173
```

Output sẽ hiển thị:
```
Serving HTTP on 0.0.0.0 port 5173 (http://0.0.0.0:5173/) ...
```

### Bước 3️⃣: Truy cập giao diện

Mở trình duyệt: **http://localhost:5173**

✨ Giao diện demo đã sẵn sàng!

---

## 🎮 Demo các tính năng

### Demo 1️⃣: Tìm kiếm phim

**Mục tiêu:** Hiểu cách hệ thống tìm kiếm phim bằng full-text search

**Các bước:**
1. Nhập từ khóa vào ô "Search" 
2. Nhấn "Search" hoặc Enter

**Ví dụ tìm kiếm:**

| Từ khóa | Kết quả mong đợi |
|---------|-----------------|
| "Avatar" | Avatar (2009), Avatar: The Way of Water |
| "Action" | Tất cả phim hành động |
| "2009" | Phim từ năm 2009 |
| "Inception" | Inception, phim liên quan |
| "Love" | Phim có "Love" trong tên |

**Luồng hoạt động:**
```
[User nhập từ khóa]
         ↓
[Frontend gửi request: GET /movies/search?keyword=Avatar]
         ↓
[FastAPI backend]
         ↓
[Query Elasticsearch]
         ↓
[Trả về kết quả (0.1-0.5s)]
```

**Xem logs:**
```bash
docker-compose logs -f api
```

---

### Demo 2️⃣: Gợi ý phim cho người dùng

**Mục tiêu:** Hiểu cách hệ thống gợi ý phim dựa trên lịch sử người dùng

**Các bước:**
1. Nhập **User ID** (ví dụ: `1`, `5`, `10`)
2. Nhập **số lượng gợi ý** (ví dụ: `5`, `10`)
3. Nhấn "Get Recommendations"

**Ví dụ:**

| User ID | Count | Kết quả |
|---------|-------|---------|
| 1 | 5 | 5 phim gợi ý cho user 1 |
| 10 | 10 | 10 phim gợi ý cho user 10 |
| 100 | 3 | 3 phim gợi ý cho user 100 |

**Luồng hoạt động (2 mức):**

**Mức 1: Cache (nhanh, < 50ms)**
```
[User yêu cầu recommendations]
         ↓
[API kiểm tra Redis cache]
         ↓
[Nếu có cache] → [Trả về ngay (cached)]
         ↓ (nếu cache miss)
[Mức 2: Fallback to Phase 4 models]
```

**Mức 2: Phase 4 Models (chậm hơn, nhưng đầy đủ)**
```
[Kiểm tra dữ liệu Phase 4 models]
    ↓
[data/models/phase04_probe/]
    ├─ user_neighbors.jsonl (user similarity)
    ├─ item_neighbors.jsonl (item similarity)
    ├─ popularity.csv
    └─ content_vectors.npy
    ↓
[Tính toán recommendations]
    ↓
[Lưu vào Redis cache]
    ↓
[Trả về kết quả]
```

**Xem chi tiết trong logs:**
```bash
docker-compose logs api | grep recommendation
```

---

### Demo 3️⃣: Theo dõi sự kiện người dùng

**Mục tiêu:** Hiểu cách hệ thống ghi nhận và xử lý sự kiện người dùng

**Các bước:**
1. Nhấp vào phim trong kết quả tìm kiếm hoặc gợi ý
2. Xem chi tiết phim
3. Nhấn các nút: **View**, **Bookmark**, **Share**, **Rate**
4. Mỗi hành động sẽ gửi event đến backend

**Các loại sự kiện:**

| Sự kiện | Ý nghĩa |
|---------|---------|
| **View** | Người dùng xem chi tiết phim |
| **Bookmark** | Lưu phim yêu thích |
| **Rate** | Đánh giá phim (1-5 sao) |
| **Share** | Chia sẻ phim |

**Luồng xử lý sự kiện:**
```
[User click "View" trên phim ID 100]
         ↓
[Frontend gửi: POST /events/track]
{
  "user_id": 1,
  "movie_id": 100,
  "event_type": "view",
  "timestamp": "2026-06-04T10:30:00Z"
}
         ↓
[FastAPI nhận và validate]
         ↓
[Ghi vào MySQL database]
         ↓
[Đẩy vào Kafka topic: user-events]
         ↓
[Spark Streaming consume events (real-time)]
         ↓
[Cập nhật user profile / popularity scores]
         ↓
[Invalidate Redis cache (để tính lại recommendations)]
```

**Xem events đang được xử lý:**
```bash
# Xem logs API
docker-compose logs -f api | grep "event"

# Xem Kafka topic
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --from-beginning
```

---

### Demo 4️⃣: Giám sát hệ thống

**Mục tiêu:** Xem metrics và performance của hệ thống

**Grafana Dashboard**
- URL: http://localhost:3000
- User: `admin`
- Password: `admin`

**Các metrics có sẵn:**
- API response time
- Request count
- Database performance
- Elasticsearch health
- Kafka lag

**Prometheus Metrics**
- URL: http://localhost:9090
- Query ví dụ:
  ```promql
  rate(http_requests_total[1m])
  histogram_quantile(0.95, http_request_duration_seconds)
  ```

---

## 🔄 Luồng hoạt động

### Luồng 1: Tìm kiếm phim

```
┌─────────────────────────────────────────────────┐
│ User nhập "Avatar" trong search box             │
└────────────────┬────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────┐
│ Frontend: GET /movies/search?keyword=Avatar     │
└────────────────┬────────────────────────────────┘
                 ↓
        ┌────────────────┐
        │   FastAPI      │
        │   Backend      │
        └────────────────┘
                 ↓
        ┌────────────────┐
        │ Elasticsearch  │
        │ Full-text      │
        │ Index          │
        └────────────────┘
                 ↓
┌─────────────────────────────────────────────────┐
│ Trả về danh sách phim khớp (< 500ms)           │
└─────────────────────────────────────────────────┘
```

### Luồng 2: Gợi ý phim

```
┌────────────────────────────────────┐
│ User yêu cầu recommendations       │
│ User ID: 5, Count: 10              │
└────────────────┬───────────────────┘
                 ↓
        ┌────────────────┐
        │  Check Redis   │
        │  Cache Layer   │
        └────┬───────────┘
             │
    ┌────────┴─────────┐
    ↓ (HIT)            ↓ (MISS)
 [Fast]           [Slow]
Cached data   Phase 4 Models
< 50ms        100-500ms
```

### Luồng 3: Ghi nhận sự kiện

```
┌────────────────────────────────┐
│ User click "Rate" (5 stars)    │
└────────────────┬───────────────┘
                 ↓
┌────────────────────────────────┐
│ POST /events/track             │
│ {user: 1, movie: 100,          │
│  event: "rate", value: 5}      │
└────────────────┬───────────────┘
                 ↓
        ┌────────────────┐
        │  FastAPI       │
        │  Validation    │
        └────────────────┘
             ↓       ↓
        MySQL    Kafka
        (DB)     (Stream)
             ↓       ↓
        ┌─────────────────────────┐
        │ Spark Streaming Job     │
        │ Real-time processing    │
        └─────────────────────────┘
```

---

## 🛑 Dừng dự án

```bash
# Dừng tất cả services
cd infra/docker
docker-compose down

# Dừng mà không xóa dữ liệu
docker-compose stop

# Dừng và xóa tất cả dữ liệu
docker-compose down -v
```

---

## 🔧 Troubleshooting

### ❌ API không khởi động

```bash
# Kiểm tra logs
docker-compose logs api

# Restart
docker-compose restart api
```

### ❌ Elasticsearch không sẵn sàng

```bash
# Chờ và kiểm tra
curl http://localhost:9200/_cluster/health

# Nếu vẫn chưa sẵn sàng
docker-compose logs elasticsearch
```

### ❌ Demo UI không kết nối API

```bash
# Kiểm tra CORS
curl -H "Origin: http://localhost:5173" \
     http://localhost:8000/health

# Nếu fail, restart API
docker-compose restart api
```

### ❌ Port đã được sử dụng

```bash
# Windows - tìm process sử dụng port 8000
netstat -ano | findstr :8000

# macOS/Linux
lsof -i :8000

# Đổi port trong .env file hoặc kill process
```

### ❌ Python dependencies error

```bash
# Cài dependencies từ requirements.txt
pip install -r backend/requirements.txt

# Hoặc dùng venv
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate      # Windows
pip install -r backend/requirements.txt
```

---

## 📚 Tài liệu chi tiết

- [Architecture Overview](docs/architecture.md) - Kiến trúc hệ thống
- [Project Overview](docs/project_overview.md) - Tổng quan dự án
- [Data Dictionary](docs/data_dictionary.md) - Mô tả dữ liệu
- [Phase 05 Runbook](docs/phase-05-runbook.md) - Hướng dẫn chạy phase 5

---

## 📊 Cấu trúc dự án

```
projectBd/
├── backend/              # FastAPI application
│   ├── app/
│   │   ├── domain/      # Business logic
│   │   ├── application/ # Use cases
│   │   ├── infrastructure/
│   │   ├── interfaces/  # API routes
│   │   └── shared/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark-jobs/          # Data processing
├── infra/docker/        # Docker Compose setup
├── data/                # Datasets
├── demo-ui/             # Frontend (HTML/CSS/JS)
└── docs/                # Documentation
```

---

## ✅ Checklist Demo

- [ ] Docker services chạy: `docker-compose ps`
- [ ] API sẵn sàng: `curl http://localhost:8000/health`
- [ ] Demo UI khởi động: `python -m http.server 5173`
- [ ] Truy cập giao diện: http://localhost:5173
- [ ] Tìm kiếm phim: Nhập "Avatar"
- [ ] Gợi ý phim: User ID 1, Count 5
- [ ] Theo dõi sự kiện: Click View/Rate/Bookmark
- [ ] Kiểm tra Grafana: http://localhost:3000

---

**Thời gian setup:** ~3-5 phút | **Thời gian demo:** ~10-15 phút

Chúc bạn trải nghiệm tốt! 🎉
