"""
Pipeline: Kafka (HTML da render) -> parse bang BeautifulSoup -> ghi Parquet ra HDFS

Chay:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --master yarn \
        product_scraper_streaming.py
"""

from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date
import pandas as pd

from spark_schema import product_crawl_schemaa


SCHEMA_FIELD_NAMES = [f.name for f in product_crawl_schemaa.product_crawl_schema.fields]


def _to_int(text):
    if text is None:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _to_float(text):
    if text is None:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------------
# Cao 1 san pham.
# Cac cot NOT NULL trong schema (title, brand, brandOrigin, madeIn, warranty,
# description, seller, sellPrice) duoc fallback ve "" / 0.0 thay vi None,
# vi Spark se bao loi neu ghi None vao cot nullable=False.
# ---------------------------------------------------------------------------
def extract_product_data(id_, html):
    url_id = _to_int(id_)
    if url_id is None:
        # urlId la NOT NULL -> khong the tao row hop le, bo qua record nay
        return pd.DataFrame(columns=SCHEMA_FIELD_NAMES)

    try:
        soup = BeautifulSoup(html, "html.parser")

        # product title
        title = soup.select_one('h1[class="sc-c0f8c612-0 dEurho"]')
        title = title.get_text(strip=True) if title else ""

        # product brand
        brand = soup.select_one('a[data-view-id="pdp_details_view_brand"]')
        brand = brand.get_text(strip=True) if brand else ""

        # product price
        price = soup.select_one("del")
        price = price.get_text(strip=True).replace(".", "").replace(",", "") if price else None
        price = _to_float(price)

        # product discount (vd "-25%" -> bo ky tu dau/cuoi la '-' va '%')
        discount = soup.select_one("div[class='product-price__discount-rate']")
        discount = discount.get_text(strip=True)[1:-1] if discount else None
        discount = _to_float(discount)

        # product sell price
        sellPrice = soup.select_one("div[class='product-price__current-price']")
        sellPrice = sellPrice.get_text(strip=True).replace(".", "").replace(",", "") if sellPrice else None
        sellPrice = _to_float(sellPrice)
        if sellPrice is None:
            sellPrice = 0.0  # NOT NULL trong schema

        # product quantity sold
        sold = soup.select_one("div[class='sc-1a46a934-3 geGARt']")
        if sold:
            tokens = sold.get_text(strip=True).split()
            sold = tokens[-1] if tokens else None
        sold = _to_int(sold)

        # product rate and product rate count
        rate_el = soup.select_one("div[class='sc-1a46a934-1 dCjKzJ']")
        productRate = None
        productRateCount = None
        if rate_el:
            prodrate = rate_el.get_text(strip=True)
            productRate = _to_float(prodrate[0:3])
            productRateCount = _to_int(prodrate[4:].replace("(", "").replace(")", "").strip())

        # product category tree
        cat_els = soup.select('a[class="breadcrumb-item"]')
        categoryTree = [c.get_text(strip=True) for c in cat_els] if cat_els else None
        category = categoryTree[-1] if categoryTree else None

        # product brand origin, made in, warranty, and additional info
        brandOrigin = ""
        madeIn = ""
        warranty = ""
        additionalInfo = []
        spans = soup.select("span")
        for span in spans:
            span_text = span.get_text(strip=True)
            sibling = span.find_next_sibling("span")
            next_span_text = sibling.get_text(strip=True) if sibling else None
            if span_text.find("Xuất xứ thương hiệu") != -1:
                brandOrigin = next_span_text or ""
            elif span_text.find("Made in") != -1:
                madeIn = next_span_text or ""
            elif span_text.find("Sản phẩm có được bảo hành không?") != -1:
                warranty = next_span_text or ""
            else:
                additionalInfo.append(f"key_{span_text}: value_{next_span_text}")

        # product description
        descriptionDiv = soup.select_one('div[class="sc-f5219d7f-0 haxTPb"]')
        description = ""
        if descriptionDiv:
            paragraphs = [p.get_text(strip=True) for p in descriptionDiv.select("p") if p.get_text(strip=True)]
            description = "\n".join(paragraphs)

        # seller name
        seller = soup.select_one('span[class="seller-name"]')
        seller = seller.get_text(strip=True) if seller else ""

        # seller rate and seller rate count
        seller_rate_el = soup.select_one('div[class="item review"]')
        sellerRate = None
        sellerRateCount = None
        if seller_rate_el:
            sr_text = seller_rate_el.get_text(strip=True)
            sellerRateCount = sr_text[4:].replace("(", "").replace(")", "").strip() if len(sr_text) > 4 else None
            sellerRate = _to_float(sr_text[:3])

        return pd.DataFrame([{
            "urlId": url_id,
            "title": title,
            "brand": brand,
            "price": price,
            "discount": discount,
            "sellPrice": sellPrice,
            "sold": sold,
            "productRate": productRate,
            "productRateCount": productRateCount,
            "category": category,
            "brandOrigin": brandOrigin,
            "madeIn": madeIn,
            "warranty": warranty,
            "description": description,
            "seller": seller,
            "sellerRate": sellerRate,
            "sellerRateCount": sellerRateCount,
            "additionalInfo": additionalInfo if additionalInfo else None,
        }])

    except Exception:
        # HTML loi cau truc -> tra ve row voi cac field NOT NULL dien "",
        # cac field nullable de None. Khong de 1 record loi lam crash ca batch.
        return pd.DataFrame([{
            "urlId": url_id,
            "title": "", "brand": "", "price": None, "discount": None,
            "sellPrice": 0.0, "sold": None, "productRate": None,
            "productRateCount": None, "category": None, "brandOrigin": "",
            "madeIn": "", "warranty": "", "description": "",
            "seller": "", "sellerRate": None, "sellerRateCount": None,
            "additionalInfo": None,
        }])

# ---------------------------------------------------------------------------
# mapInPandas nhan iterator cua pandas.DataFrame (theo tung batch), KHONG
# phai iterator cua tung tuple (id, html) -- day la loi hay gap nhat.
# Phai dung zip() tren 2 cot de ghep lai thanh tung cap (id, html).
# ---------------------------------------------------------------------------
def extract_batch(iterator):
    for pdf in iterator:
        results = []
        for id_, html, ts in zip(pdf["urlId"], pdf["html"], pdf["timestamp"]):
            row_df = extract_product_data(id_, html)
            row_df["timestamp"] = ts
            results.append(row_df)
        if results:
            yield pd.concat(results, ignore_index=True)
        else:
            yield pd.DataFrame(columns=SCHEMA_FIELD_NAMES + ["timestamp"])


spark = SparkSession.builder.appName("Product Extractor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "192.168.196.128:9092"
KAFKA_TOPIC = "product_data_changes"
HDFS_OUTPUT_PATH = "hdfs://192.168.196.128:9000/user/ubuntu/datalake/product_crawl/"
HDFS_CHECKPOINT_PATH = "hdfs://192.168.196.128:9000/tmp/spark/checkpoints/product_crawl/"

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# key Kafka dung lam urlId (phai la so nguyen), value la HTML
html_df = raw_df.selectExpr(
    "CAST(key AS STRING) as urlId",
    "CAST(value AS STRING) as html",
    'timestamp'
)

parsed_df = html_df.mapInPandas(extract_batch, schema=product_crawl_schemaa.product_crawl_schema)

parsed_df = parsed_df.withColumn("date", to_date(date_format("timestamp", "dd-MM-yyyy")))

query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", HDFS_OUTPUT_PATH)
    .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
    .partitionBy("date")
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start()
)

query.awaitTermination()