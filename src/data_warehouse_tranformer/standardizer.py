from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from argparse import ArgumentParser

parser = ArgumentParser(description="Product Standardizer")
parser.add_argument("--partition", type=str, required=True, help="Partition to process (e.g., '2026-01-01')")

args = parser.parse_args()

spark = SparkSession.builder.appName("Product Standardizer").getOrCreate()

df = spark.read.parquet(f"hdfs://10.128.0.18:9000/user/pmquanbackup/datalake/product_crawl/date={args.partition}/")

prod_window = Window.partitionBy("urlId").orderBy(desc("timestamp"))

product_snapshot_df = df.withColumn("row_num", dense_rank().over(prod_window)).filter(col("row_num") == 1).cache()

brand_df = product_snapshot_df.select(
    product_snapshot_df.brand,
    product_snapshot_df.brandOrigin
).dropDuplicates(["brand"])\
.withColumn("brandId", xxhash64("brand"))\
.cache()

brand_df.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:clickhouse://10.128.0.16:8123/ecommerce_warehouse")\
.option("dbtable", "brandDim")\
.option("user", "default")\
.option("password", "123456")\
.option("driver", "com.clickhouse.jdbc.ClickHouseDriver")\
.option("truncate", "true")\
.save()

seller_df = product_snapshot_df.select(
    product_snapshot_df.seller,
    product_snapshot_df.sellerRate,
    product_snapshot_df.sellerRateCount,
).dropDuplicates(["seller"])\
.withColumn("sellerId", xxhash64("seller"))\
.cache()

seller_df.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:clickhouse://10.128.0.16:8123/ecommerce_warehouse")\
.option("dbtable", "sellerDim")\
.option("user", "default")\
.option("password", "123456")\
.option("driver", "com.clickhouse.jdbc.ClickHouseDriver")\
.option("truncate", "true")\
.save()

product_df = product_snapshot_df.select(
    product_snapshot_df.urlId,
    product_snapshot_df.title,
    product_snapshot_df.category,
    product_snapshot_df.madeIn,
    product_snapshot_df.warranty
).dropDuplicates(["urlId"])

product_df.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:clickhouse://10.128.0.16:8123/ecommerce_warehouse")\
.option("dbtable", "productDim")\
.option("user", "default")\
.option("password", "123456")\
.option("driver", "com.clickhouse.jdbc.ClickHouseDriver")\
.option("truncate", "true")\
.save()

product_snapshot_df = product_snapshot_df.join(brand_df.select("brandId", "brand"), product_snapshot_df.brand == brand_df.brand, how="left")\
    .join(seller_df.select("sellerId", "seller"), product_snapshot_df.seller == seller_df.seller, how="left")\
    .drop("brand", "title", "seller", "row_num", "brandOrigin", "sellerRate", "sellerRateCount", "description", "additionalInfo")

product_snapshot_df.write.mode("overwrite")\
    .format("jdbc")\
    .option("url", "jdbc:clickhouse://10.128.0.16:8123/ecommerce_warehouse")\
    .option("dbtable", "productSnapshotFact")\
    .option("user", "default")\
    .option("password", "123456")\
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")\
    .option("truncate", "true")\
    .save()
    
product_snapshot_df.unpersist()
brand_df.unpersist()
seller_df.unpersist()
spark.stop()