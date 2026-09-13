from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, ArrayType, TimestampType

product_crawl_schema = StructType(
    [
        StructField("urlId", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("sellPrice", DoubleType(), False),
        StructField("sold", IntegerType(), True),
        StructField("productRate", DoubleType(), True),
        StructField("productRateCount", IntegerType(), True),
        StructField("categoryTree", ArrayType(StringType()), True),
        StructField("brandOrigin", StringType(), False),
        StructField("madeIn", StringType(), False),
        StructField("warranty", StringType(), False),
        StructField("description", StringType(), False),
        StructField("seller", StringType(), False),
        StructField("sellerRate", DoubleType(), True),
        StructField("sellerRateCount", StringType(), True),
        StructField("additionalInfo", ArrayType(StringType()), True),
        StructField("timestamp", TimestampType(), False)
    ]
)