# Capstone Project - E-commerce Data Platform

Dự án này xây dựng một hệ thống thu thập, xử lý, chuẩn hóa và trực quan hóa dữ liệu thương mại điện tử từ website Tiki, đồng thời kết hợp giám sát tài nguyên máy chủ bằng Google Cloud Monitoring và dashboard phân tích doanh nghiệp. Mục tiêu là tạo ra một pipeline dữ liệu hoàn chỉnh từ crawl dữ liệu sản phẩm, lưu trữ trong data warehouse, đến dashboard báo cáo và monitoring.

## 1. Tổng quan dự án

Dự án được thiết kế theo mô hình data pipeline hiện đại với các thành phần chính:

- Crawler thu thập URL và HTML sản phẩm từ Tiki
- Kafka làm hàng đợi dữ liệu giữa crawler và extractor
- Spark Streaming xử lý HTML và trích xuất trường dữ liệu cấu trúc
- HDFS/Parquet lưu dữ liệu thô theo từng ngày
- Data standardizer chuẩn hóa dữ liệu và ghi vào ClickHouse
- Streamlit dashboard hiển thị phân tích kinh doanh và giám sát VM
- Airflow DAG tự động hóa job nạp warehouse theo lịch

Tổng thể, hệ thống giúp theo dõi và phân tích các yếu tố như:

- Sản phẩm bán chạy, danh mục, thương hiệu, nhà bán hàng
- Số lượng sản phẩm theo danh mục
- Tín hiệu thay đổi dữ liệu sản phẩm theo thời gian
- Tình trạng tài nguyên máy chủ (CPU, RAM, Disk, Network)

---

## 2. Kiến trúc hệ thống

Luồng dữ liệu chính của dự án như sau:

1. Discovery crawler tìm và lưu URL sản phẩm vào bảng crawl_state
2. Refresh crawler lấy danh sách URL cần cập nhật, mở trang Tiki, cuộn trang và thu thập HTML
3. Dữ liệu HTML được gửi lên Kafka topic `product_data_changes`
4. Spark Streaming đọc Kafka, parse HTML bằng BeautifulSoup
5. Dữ liệu trích xuất được ghi vào HDFS theo partition ngày
6. Job `standardizer.py` đọc dữ liệu Parquet, chuẩn hóa và ghi sang ClickHouse
7. Dashboard Streamlit đọc dữ liệu từ ClickHouse và Google Monitoring API để hiển thị báo cáo và metric
8. Airflow DAG định kỳ chạy job chuẩn hóa warehouse hàng ngày

Mô hình khái quát:

```text
Tiki Website
   ↓
Crawler (Discovery / Refresh)
   ↓
Kafka Topic: product_data_changes
   ↓
Spark Streaming + BeautifulSoup
   ↓
Parquet on HDFS
   ↓
Spark Standardizer
   ↓
ClickHouse / Data Warehouse
   ↓
Streamlit dashboard + Google Monitoring
```

---

## 3. Cấu trúc thư mục

```text
Capstone_project/
├── LICENSE
├── README.md
├── src/
│   ├── airflow_dag/
│   │   └── daily_load_warehouse/
│   │       ├── daily_load_warehouse.py
│   │       └── script.sh
│   ├── crawler/
│   │   ├── base_crawler.py
│   │   ├── discovery_crawler.py
│   │   ├── refresh_crawler.py
│   │   ├── tiki_rendered.html
│   ├── dashboard/
│   │   ├── analysis_page.py
│   │   ├── data_wh_func.py
│   │   ├── google_metric_func.py
│   │   ├── main_dashboard.py
│   │   └── vms_monitoring_page.py
│   ├── data_warehouse_tranformer/
│   │   └── standardizer.py
│   ├── extracter/
│   │   ├── product_extractor.py
│   │   ├── product_extractor.py.old
│   │   ├── product_extractorr.py
│   ├── spark_schema/
│   │   └── product_crawl_schemaa.py
│   └── warehouse_sript/
│       └── ddl_.sql
└── .env
```

---

## 4. Thành phần chính

### 4.1. Crawler

#### `src/crawler/base_crawler.py`

- Base class cho tất cả crawler
- Dùng Selenium + Microsoft Edge WebDriver
- Hỗ trợ:
  - `headless` mode
  - `user_data_dir`/profile riêng
  - tự cleanup process orphan và lock file
  - restart driver khi RAM hoặc trạng thái browser không ổn định
- Có cơ chế đảm bảo đóng process sạch hơn khi chương trình thoát bất thường

#### `src/crawler/discovery_crawler.py`

- Dùng để khám phá URL sản phẩm hoặc các trang liên quan từ Tiki
- Tự động crawl theo luồng liên kết sản phẩm
- Lưu URL vào bảng `crawl_state` trong PostgreSQL/warehouse
- Mục đích: xây nền tảng dữ liệu nguồn cho các lần refresh sau

#### `src/crawler/refresh_crawler.py`

- Crawler chính để cập nhật dữ liệu sản phẩm hiện có
- Dùng Kafka producer để gửi HTML raw lên topic `product_data_changes`
- Có cơ chế:
  - cuộn trang để đợi render dữ liệu
  - clear cache định kỳ
  - restart driver khi RAM quá cao hoặc quá nhiều URL đã crawl
  - poll Kafka liên tục để đảm bảo ack được xử lý

### 4.2. Trích xuất dữ liệu

#### `src/extracter/product_extractor.py`

- Spark Streaming job đọc dữ liệu từ Kafka
- Mỗi record là `(urlId, html, timestamp)`
- Dùng BeautifulSoup để parse HTML và trích xuất các trường như:
  - title
  - brand
  - price
  - discount
  - sellPrice
  - sold
  - productRate
  - categoryTree
  - description
  - seller
  - warranty
  - madeIn
  - sellerRate
  - additionalInfo
- Dữ liệu hợp lệ được ghi ra Parquet theo partition `date`

### 4.3. Warehouse và chuẩn hóa dữ liệu

#### `src/warehouse_sript/ddl_.sql`

- Khởi tạo bảng dữ liệu trong ClickHouse:
  - `brandDim`
  - `sellerDim`
  - `productDim`
  - `productSnapshotFact`

#### `src/data_warehouse_tranformer/standardizer.py`

- Đọc dữ liệu Parquet từ HDFS
- Chọn snapshot mới nhất cho từng `urlId`
- Tạo các bảng dimension:
  - `brandDim`
  - `sellerDim`
  - `productDim`
- Tạo bảng fact `productSnapshotFact` chứa dữ liệu sản phẩm theo thời gian
- Viết dữ liệu lên ClickHouse qua JDBC

### 4.4. Airflow automation

#### `src/airflow_dag/daily_load_warehouse/daily_load_warehouse.py`

- DAG `daily_load_warehouse`
- Chạy hàng ngày theo cron `@daily`
- Bước chính:
  - start
  - load_warehouse
  - end

#### `src/airflow_dag/daily_load_warehouse/script.sh`

- Script shell chạy `spark-submit` cho job chuẩn hóa dữ liệu
- Dùng Hadoop/YARN và ClickHouse JDBC

### 4.5. Dashboard

#### `src/dashboard/main_dashboard.py`

- Trang chính của ứng dụng Streamlit
- Mount các page con:
  - `Analysis` page
  - `Monitoring` page

#### `src/dashboard/analysis_page.py`

- Hiển thị các chỉ số kinh doanh chính:
  - Tổng số sản phẩm
  - Tổng số thương hiệu
  - Tổng số người bán
  - Top danh mục bán chạy
  - Top nhà bán hàng bán chạy
  - Top sản phẩm tiềm năng
- Dùng Plotly để trực quan hóa

#### `src/dashboard/data_wh_func.py`

- Hàm truy vấn dữ liệu từ ClickHouse để phục vụ dashboard
- Các truy vấn chính:
  - số lượng sản phẩm, thương hiệu, seller
  - top category
  - top seller
  - top potential products

#### `src/dashboard/google_metric_func.py`

- Kết nối với Google Cloud Monitoring API
- Lấy metric từ Compute Engine: CPU, Disk, Memory, Network
- Trả về DataFrame chuẩn để vẽ biểu đồ

#### `src/dashboard/vms_monitoring_page.py`

- Trang giám sát VM trên Google Cloud
- Cho phép chọn khoảng thời gian và loại tài nguyên
- Biểu diễn các chart của:
  - CPU utilization
  - System Load per vCPU
  - vCPU Core Usage
  - Disk usage
  - Memory usage
  - Network throughput

---

## 5. Công nghệ sử dụng

- Python 3.x
- Selenium + Microsoft Edge WebDriver
- Kafka
- Spark Structured Streaming
- BeautifulSoup
- HDFS / Parquet
- ClickHouse
- SQLAlchemy
- Streamlit
- Plotly
- Airflow
- Google Cloud Monitoring + Compute Engine API
- PostgreSQL / relational DB for crawl_state and scheduling metadata

---

## 6. Môi trường & biến môi trường

Dự án sử dụng file `.env` để quản lý tham số cấu hình. Một số biến quan trọng có thể bao gồm:

```env
POSTGRES_URL=...
CLICKHOUSE_URL=...
GOOGLE_CLOUD_PROJECT=...
```

Cần đảm bảo:

- Chrome/Edge Driver đã được cài và đường dẫn đúng
- Kafka broker đang hoạt động
- HDFS và Spark cluster sẵn sàng
- ClickHouse đang chạy và schema đã được tạo
- Google Cloud credentials đã được cấu hình nếu dùng monitoring

---

## 7. Cài đặt và chạy dự án

### 7.1. Yêu cầu môi trường

- Python >= 3.10
- Java (nếu chạy Spark)
- Apache Kafka
- Spark
- HDFS
- ClickHouse
- Microsoft Edge + WebDriver
- Google Cloud SDK / service account credentials

### 7.2. Cài đặt thư viện Python

```bash
pip install -r requirements.txt
```

Nếu chưa có `requirements.txt`, bạn có thể cài đặt các gói cần thiết như:

```bash
pip install selenium psutil python-dotenv sqlalchemy pandas plotly streamlit kafka-python confluent-kafka beautifulsoup4 pyspark google-cloud-monitoring google-cloud-compute clickhouse-driver
```

### 7.3. Khởi tạo schema warehouse

Chạy file SQL trong `src/warehouse_sript/ddl_.sql` để tạo bảng warehouse trong ClickHouse.

### 7.4. Chạy crawler

#### Discovery crawler

```bash
cd src/crawler
python discovery_crawler.py
```

#### Refresh crawler

```bash
cd src/crawler
python refresh_crawler.py
```

### 7.5. Chạy extractor Spark Streaming

```bash
cd src/extracter
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master yarn \
  product_extractor.py
```

### 7.6. Chạy standardizer

```bash
cd src/data_warehouse_tranformer
spark-submit standardizer.py --partition 2026-09-14
```

### 7.7. Chạy dashboard

```bash
cd src/dashboard
streamlit run main_dashboard.py
```

---

## 8. Luồng dữ liệu thực tế

### 8.1. Discovery crawl

- Tìm URL sản phẩm mới hoặc cần kiểm tra lại
- Dùng `crawl_state` để quản lý trạng thái và lịch crawl
- Đảm bảo không trùng lặp URL

### 8.2. Refresh crawl

- Đọc URL trong trạng thái cần refresh
- Mở từng trang sản phẩm
- Cuộn xuống cuối trang để đợi dữ liệu render
- Bắt HTML và gửi lên Kafka để xử lý tiếp

### 8.3. Extract & normalize

- Parse HTML bằng BeautifulSoup
- Chuyển từ HTML sang dữ liệu có cấu trúc rõ ràng
- Ghi lại partition ngày để lưu trữ lịch sử

### 8.4. Warehouse analytics

- Chọn số liệu mới nhất cho từng `urlId`
- Ghép `brand`, `seller`, `product` vào các dimension và fact tables
- Tạo nền tảng cho BI / dashboard

### 8.5. Business intelligence

- Dashboard hiển thị doanh số, xu hướng, top category, top seller
- Monitoring page theo dõi tài nguyên VM từ GCP

---

## 9. Giải thích chức năng kinh doanh

Dự án này không chỉ là một project craw data đơn thuần mà còn cung cấp một nền tảng hỗ trợ ra quyết định cho vận hành thương mại điện tử:

- Phân tích sản phẩm có triển vọng
- Xác định nhà bán hàng mạnh, danh mục bán chạy
- Theo dõi xu hướng thay đổi của sản phẩm theo thời gian
- Giám sát hệ thống và tài nguyên máy chủ để đảm bảo pipeline luôn hoạt động

Về mặt thực tiễn, dự án có thể mở rộng để trở thành hệ thống thương mại điện tử tracking hoặc dashboard phân tích online retail.

---

## 10. Lưu ý quan trọng

- Đây là dự án học tập / nghiên cứu trong môi trường cụ thể, nên cấu hình địa chỉ IP, Kafka broker, HDFS, ClickHouse, và đường dẫn driver có thể thay đổi theo môi trường deployment.
- Một số file trong dự án là phiên bản cũ hoặc test (`*.old`, `test.ipynb`, `test.py`) dùng để thử nghiệm và có thể không chạy trong môi trường production.
- Một số phần của code được viết theo hướng tối ưu hóa trong môi trường server riêng, vì vậy cần kiểm tra lại biến môi trường và đường dẫn trước khi triển khai.
- Project đang chứa cả logic crawl và logic monitoring nên cần hiểu rõ từng module để tránh nhầm lẫn giữa `crawler`, `extractor`, `warehouse`, và `dashboard`.

---

## 11. Kết luận

Capstone Project này là một hệ thống dữ liệu end-to-end cho mảng thương mại điện tử, kết hợp crawl dữ liệu thị trường, xử lý streaming, lưu trữ warehouse và dashboard phân tích. Dự án cho thấy cách xây dựng một pipeline dữ liệu thực tế từ đầu vào là website Tiki, qua Kafka và Spark, đến các báo cáo BI trên Streamlit và giám sát cloud resource.

Nếu tiếp tục phát triển, dự án có thể mở rộng thêm các tính năng như:

- dự báo xu hướng bán hàng
- alert khi giá hoặc seller thay đổi
- dashboard theo thời gian thực
- tích hợp với dữ liệu ngoài như marketplace khác
- tự động hóa pipeline theo event-driven architecture

---

## 12. Tài liệu tham khảo nhanh

- Crawler: `src/crawler/`
- Extractor: `src/extracter/`
- Warehouse transform: `src/data_warehouse_tranformer/`
- Dashboard: `src/dashboard/`
- Airflow: `src/airflow_dag/`
- Schema warehouse: `src/warehouse_sript/ddl_.sql`

Bản README này được viết dựa trên cấu trúc code thực tế của dự án và mô tả quy trình hoạt động của từng thành phần chính.