import boto3
import sys
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import year, month, dayofmonth, date_format

# Initialize Spark and Glue Context
spark = SparkSession.builder.appName("FashionEcommerceETL").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# ---------------------------------------------------------
# 1. Extraktion aus dem AWS Glue Catalog
# ---------------------------------------------------------
orders_df = glueContext.create_dynamic_frame.from_catalog(database="fashion_ecommerce_db", table_name="orders").toDF()
users_df = glueContext.create_dynamic_frame.from_catalog(database="fashion_ecommerce_db", table_name="users").toDF()
products_df = glueContext.create_dynamic_frame.from_catalog(database="fashion_ecommerce_db", table_name="products").toDF()

# ---------------------------------------------------------
# 2. Transformation (Aufbau des Sternschemas)
# ---------------------------------------------------------

# Erstellung der Zeitdimension (dim_time)
dim_time_df = orders_df.select(
    date_format(orders_df.timestamp, "yyyyMMdd").cast("string").alias("date_id"),
    year(orders_df.timestamp).alias("year"),
    month(orders_df.timestamp).alias("month"),
    dayofmonth(orders_df.timestamp).alias("day")
).distinct()

# Erstellung der Produktdimension (dim_products)
dim_products_df = products_df.select(
    products_df.product_id.alias("productkey"),
    products_df.product_name,
    products_df.category,
    products_df.brand,
    products_df.price,
    products_df.discount,
    products_df.final_price,
    products_df.luxury_brand
)

# Erstellung der Kundendimension (dim_users)
dim_users_df = users_df.select(
    users_df.user_id.alias("userkey"),
    users_df.first_name,
    users_df.last_name,
    users_df.country,
    users_df.premium_member
)

# Transformation der Faktentabelle (fact_orders)
fact_orders_df = orders_df \
    .withColumn("date_id", date_format(orders_df.timestamp, "yyyyMMdd").cast("string")) \
    .join(dim_users_df, orders_df.user_id == dim_users_df.userkey, "left") \
    .join(dim_products_df, orders_df.product_id == dim_products_df.productkey, "left") \
    .join(dim_time_df, "date_id", "left") \
    .select(
        orders_df.order_id.alias("order_id"),
        dim_users_df.userkey,
        dim_users_df.country,
        dim_products_df.productkey,
        dim_products_df.category,
        dim_time_df.date_id,
        dim_time_df.year,
        dim_time_df.month,
        orders_df.quantity,
        orders_df.total_price,
        orders_df.order_status
    )

# ---------------------------------------------------------
# 3. Laden in Amazon S3 (Parquet Format)
# ---------------------------------------------------------
common_s3_path = "s3://data-fashion-ecommerce/processed-data/"
s3 = boto3.client("s3")
bucket_name = "data-fashion-ecommerce"

def save_as_single_file(df, table_name):
    """Speichert DataFrame als einzelne Datei in S3, um Partitions-Chaos zu vermeiden"""
    temp_path = f"{common_s3_path}{table_name}/temp_output/"
    final_path = f"processed-data/{table_name}/"
    
    # Schreibe die Daten als eine einzige Datei
    df.repartition(1).write.mode("overwrite").parquet(temp_path)
    
    # Umbenennung der Datei zur Standardisierung
    temp_folder = f"processed-data/{table_name}/temp_output/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_folder)
    
    for obj in response.get("Contents", []):
        file_key = obj["Key"]
        if file_key.endswith(".parquet"):
            new_key = final_path + f"{table_name}.parquet"
            s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{file_key}", Key=new_key)
            s3.delete_object(Bucket=bucket_name, Key=file_key)

# Speichere alle Tabellen
save_as_single_file(fact_orders_df, "fact_orders")
save_as_single_file(dim_time_df, "dim_time")
save_as_single_file(dim_users_df, "dim_users")
save_as_single_file(dim_products_df, "dim_products")

print("ETL Process Completed Successfully!")