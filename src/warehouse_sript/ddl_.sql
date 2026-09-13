CREATE DATABASE IF NOT EXISTS ecommerce_warehouse;

CREATE TABLE IF NOT EXISTS ecommerce_warehouse.brandDim
(
    brand        String,
    brandOrigin  String,
    brandId      Int64
)
ENGINE = MergeTree()
PRIMARY KEY (brandId)
ORDER BY (brandId);

CREATE TABLE IF NOT EXISTS ecommerce_warehouse.sellerDim
(
    seller           String,
    sellerRate       Nullable(Float64),
    sellerRateCount  Nullable(String),
    sellerId         Int64
)
ENGINE = MergeTree()
PRIMARY KEY (sellerId)
ORDER BY (sellerId);

CREATE TABLE IF NOT EXISTS ecommerce_warehouse.productDim
(
    urlId     Int32,
    title     String,
    category  Nullable(String),
    madeIn    String,
    warranty  String
)
ENGINE = MergeTree()
PRIMARY KEY (urlId)
ORDER BY (urlId);

CREATE TABLE IF NOT EXISTS ecommerce_warehouse.productSnapshotFact
(
    urlId             Int32,
    title             String,
    price             Nullable(Float64),
    discount          Nullable(Float64),
    sellPrice          Float64,
    sold              Nullable(Int32),
    productRate       Nullable(Float64),
    productRateCount  Nullable(Int32),
    category          Nullable(String),
    madeIn            String,
    warranty          String,
    timestamp         DateTime,
    brandId           Nullable(Int64),
    sellerId          Nullable(Int64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (urlId, timestamp);