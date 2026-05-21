Dưới đây là một thiết kế thư mục theo Clean Architecture phù hợp với dự án tìm kiếm và đề xuất phim dùng MovieLens, Spark, Kafka, Elasticsearch, MySQL, Redis, MinIO, Docker.

Mục tiêu của cấu trúc này là tách rõ:

Domain: logic nghiệp vụ cốt lõi
Application: use case
Infrastructure: kết nối DB, Elasticsearch, Redis, Kafka, MinIO
Interface / Presentation: API endpoint, request/response
Data pipeline: Spark jobs, streaming jobs, ETL
1. Cấu trúc tổng thể đề xuất
movie-recommendation-platform/
│
├── backend/
│   ├── app/
│   │   ├── domain/
│   │   │   ├── entities/
│   │   │   ├── value_objects/
│   │   │   ├── repositories/
│   │   │   └── services/
│   │   │
│   │   ├── application/
│   │   │   ├── use_cases/
│   │   │   ├── dto/
│   │   │   └── ports/
│   │   │
│   │   ├── infrastructure/
│   │   │   ├── persistence/
│   │   │   ├── elasticsearch/
│   │   │   ├── redis/
│   │   │   ├── kafka/
│   │   │   ├── minio/
│   │   │   └── config/
│   │   │
│   │   ├── interfaces/
│   │   │   ├── api/
│   │   │   │   ├── routes/
│   │   │   │   ├── schemas/
│   │   │   │   └── dependencies/
│   │   │   └── presenters/
│   │   │
│   │   └── shared/
│   │       ├── exceptions/
│   │       ├── constants/
│   │       ├── utils/
│   │       └── logging/
│   │
│   ├── main.py
│   └── requirements.txt
│
├── spark-jobs/
│   ├── batch/
│   │   ├── extract_movies.py
│   │   ├── transform_movies.py
│   │   ├── build_features.py
│   │   └── train_recommender.py
│   │
│   ├── streaming/
│   │   ├── consume_kafka_events.py
│   │   ├── update_user_profile.py
│   │   └── realtime_recommendation.py
│   │
│   └── common/
│       ├── spark_session.py
│       ├── schemas.py
│       └── transformations.py
│
├── data/
│   ├── raw/
│   ├── cleaned/
│   ├── features/
│   └── models/
│
├── infra/
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   ├── elasticsearch/
│   │   ├── kafka/
│   │   ├── mysql/
│   │   ├── redis/
│   │   └── minio/
│   │
│   └── scripts/
│       ├── init_db.sql
│       ├── seed_data.py
│       └── setup_indices.py
│
├── docs/
│   ├── architecture.md
│   ├── api.md
│   └── data_flow.md
│
└── README.md
2. Ý nghĩa từng lớp trong Clean Architecture
2.1 domain/ — lõi nghiệp vụ

Đây là nơi chứa logic “thuần”, không phụ thuộc framework hay công nghệ.

Chứa gì:
entities/: đối tượng nghiệp vụ chính
value_objects/: kiểu dữ liệu bất biến
repositories/: interface mô tả cách truy xuất dữ liệu
services/: nghiệp vụ phức tạp không thuộc riêng entity nào
Ví dụ:
Movie
User
Rating
Recommendation
Ví dụ lý thuyết:

Nếu hệ thống nói rằng:

“Một phim có thể được gợi ý vì nó có cùng genre, cùng actor, hoặc có điểm tương đồng cao”

thì quy tắc này nên nằm trong domain/services/, không nên nằm trong controller.

2.2 application/ — use case

Đây là tầng điều phối nghiệp vụ.

Chứa gì:
use_cases/: từng hành động của hệ thống
dto/: dữ liệu vào/ra cho use case
ports/: interface giao tiếp với bên ngoài
Ví dụ use case:
SearchMovieUseCase
GetRecommendationUseCase
IngestMovieDataUseCase
TrackUserEventUseCase
Ví dụ:

Khi user gọi:

GET /movies/search?q=avatar

thì API sẽ gọi SearchMovieUseCase, use case này mới quyết định:

kiểm tra cache Redis trước
nếu không có thì query Elasticsearch
chuẩn hóa kết quả trả về
2.3 infrastructure/ — triển khai kỹ thuật

Đây là nơi gắn code vào công nghệ thật.

Chứa gì:
persistence/: MySQL repository implementation
elasticsearch/: index, query, mapping
redis/: cache, session, leaderboard
kafka/: producer/consumer
minio/: đọc/ghi file dữ liệu
config/: cấu hình môi trường
Ví dụ:
MySQLMovieRepository
ElasticsearchMovieSearchRepository
RedisRecommendationCache
KafkaEventPublisher
MinIOStorageService
Tư duy:

domain/repositories/MovieRepository chỉ là interface.
infrastructure/persistence/mysql_movie_repository.py mới là phần code thật dùng SQLAlchemy hoặc connector.

2.4 interfaces/ — giao diện ngoài

Đây là lớp tiếp nhận request từ bên ngoài.

Chứa gì:
api/routes/: route REST
api/schemas/: request/response model
api/dependencies/: auth, inject service
presenters/: format kết quả trả ra
Ví dụ:
movies.py
recommendations.py
auth.py
Ví dụ cụ thể:

Controller không nên viết logic search.
Nó chỉ làm việc kiểu:

nhận request
validate input
gọi use case
trả response
2.5 shared/ — dùng chung

Chứa các phần được dùng ở nhiều nơi.

Chứa gì:
exceptions/
constants/
utils/
logging/
Ví dụ:
NotFoundError
ValidationError
paginate()
setup_logger()
3. Thiết kế thư mục cho phần Spark

Vì dự án của bạn có ETL và processing bằng Spark, nên nên tách riêng khỏi backend API.

3.1 spark-jobs/batch/

Chứa job xử lý batch.

Ví dụ:
đọc MovieLens từ MinIO
làm sạch dữ liệu
build feature
train model recommendation
Các file hợp lý:
extract_movies.py
transform_movies.py
build_features.py
train_recommender.py
3.2 spark-jobs/streaming/

Chứa job realtime.

Ví dụ:
đọc event từ Kafka
update user profile
update cache Redis
ghi log tương tác
File gợi ý:
consume_kafka_events.py
update_user_profile.py
realtime_recommendation.py
3.3 spark-jobs/common/

Chứa code dùng chung cho batch và streaming.

Ví dụ:
tạo SparkSession
schema dữ liệu
hàm normalize text
xử lý null
4. Cấu trúc theo chức năng thực tế

Tôi khuyên bạn chia theo module nghiệp vụ bên trong từng layer.

Ví dụ trong domain/entities/:

domain/entities/
├── movie.py
├── user.py
├── rating.py
├── watch_event.py
└── recommendation.py

Trong application/use_cases/:

application/use_cases/
├── search_movie.py
├── get_recommendation.py
├── ingest_movie_data.py
├── process_user_event.py
└── sync_search_index.py

Trong interfaces/api/routes/:

interfaces/api/routes/
├── movies.py
├── recommendations.py
├── users.py
└── health.py
5. Ví dụ luồng chạy theo Clean Architecture
Trường hợp 1: Tìm kiếm phim
Luồng:
Client → API route → SearchMovieUseCase → SearchRepository interface → Elasticsearch implementation → Response
Giải thích:
Route không biết Elasticsearch là gì
Use case không phụ thuộc thư viện Elasticsearch cụ thể
Infrastructure mới chứa code kết nối thật

Đây là điểm cốt lõi của Clean Architecture.

Trường hợp 2: Gợi ý phim
Luồng:
Client → Recommendation API → GetRecommendationUseCase → Redis cache
                                           ↓
                              nếu miss → MySQL / model / Spark output
Ý nghĩa:
Nếu đã có cache thì trả ngay
Nếu chưa có thì lấy từ mô hình hoặc bảng kết quả đã tính sẵn
6. Một cấu trúc thực tế hơn, dễ làm cho BTL

Nếu bạn muốn code gọn và không quá phức tạp, tôi đề xuất bản rút gọn này:

movie-system/
├── app/
│   ├── domain/
│   ├── application/
│   ├── infrastructure/
│   ├── interfaces/
│   └── shared/
│
├── spark/
│   ├── batch/
│   ├── streaming/
│   └── common/
│
├── infra/
│   └── docker/
│
├── data/
├── docs/
└── README.md

Cách này vẫn đúng tư duy Clean Architecture nhưng dễ trình bày trong báo cáo hơn.

7. Khuyến nghị thực tế cho dự án của bạn

Với bài toán này, tôi khuyên dùng cấu trúc sau:

Backend API: FastAPI
Domain/Application: pure Python
Infrastructure: MySQL, Redis, Elasticsearch, Kafka, MinIO
Data processing: PySpark
Deploy: Docker Compose
8. Một ví dụ module cụ thể
domain/entities/movie.py

Chứa đối tượng phim:

@dataclass
class Movie:
    movie_id: int
    title: str
    genres: list[str]
    overview: str | None = None
application/use_cases/search_movie.py

Chứa logic tìm kiếm:

class SearchMovieUseCase:
    def __init__(self, search_repo):
        self.search_repo = search_repo

    def execute(self, keyword: str):
        return self.search_repo.search(keyword)
infrastructure/elasticsearch/movie_search_repository.py

Chứa query Elasticsearch thật.

9. Nguyên tắc quan trọng khi tổ chức thư mục

Có 3 nguyên tắc nên giữ chặt:

Domain không phụ thuộc framework
không import FastAPI, SQLAlchemy, Elasticsearch trong domain
Application chỉ biết interface
use case gọi repository interface, không gọi DB trực tiếp
Infrastructure là nơi “dính công nghệ”
MySQL, Kafka, Redis, MinIO, Elasticsearch nằm ở đây
10. Gợi ý cuối cùng

Nếu đây là BTL, bạn nên ưu tiên:

dễ bảo vệ
dễ demo
dễ giải thích kiến trúc

Nên trình bày theo mô hình:

Client → API → Use Case → Repository Interface → Infrastructure Implementation

đó là câu chuyện rất đẹp khi thuyết trình Clean Architecture.

Nếu cần, tôi có thể viết tiếp cho bạn:

cấu trúc thư mục chuẩn FastAPI + Clean Architecture chi tiết hơn
skeleton code mẫu cho từng folder
hoặc 
sơ đồ kiến trúc để đưa thẳng vào báo cáo.