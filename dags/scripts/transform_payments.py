from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

LAKE_DIR   = "/opt/airflow/data_lake"
OUTPUT_DIR = f"{LAKE_DIR}/transformed"


def run_transform():
    spark = SparkSession.builder \
        .appName("DustiniaDelixia_Finance_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Membaca parquet dari data lake...")
    orders    = spark.read.parquet(f"file://{LAKE_DIR}/orders.parquet")
    payments  = spark.read.parquet(f"file://{LAKE_DIR}/payments.parquet")
    customers = spark.read.parquet(f"file://{LAKE_DIR}/customers.parquet")
    items     = spark.read.parquet(f"file://{LAKE_DIR}/items.parquet")

    print("Membuat fact_payments...")
    order_total = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_payment"),
        F.max("payment_installments").alias("max_installments")
    )

    primary_payment = payments.filter(F.col("payment_sequential") == 1) \
        .select("order_id", "payment_type")

    fact = orders.join(order_total, "order_id") \
        .join(primary_payment, "order_id", "left") \
        .join(
            customers.select("customer_id", "customer_unique_id", "customer_state"),
            "customer_id"
        )

    fact.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/fact_payments.parquet")
    print(f"  fact_payments: {fact.count():,} baris (semua status)")

    print("Membuat customer_segments...")
    fact_delivered = fact.filter(F.col("order_status") == "delivered")

    cust_spend = fact_delivered.groupBy(
        "customer_id", "customer_unique_id", "customer_state"
    ).agg(
        F.sum("total_payment").alias("total_spent"),
        F.count("order_id").alias("order_count")
    )

    cust_segments = cust_spend.withColumn(
        "segment",
        F.when(F.col("total_spent") >= 400, "Premium")
        .when(F.col("total_spent") >= 200, "High")
        .otherwise("Regular")
    )

    cust_segments.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/customer_segments.parquet")
    print(f"  customer_segments: {cust_segments.count():,} baris")

    print("Membuat revenue_by_state...")
    rev_state = fact_delivered.groupBy("customer_state").agg(
        F.sum("total_payment").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("total_payment").alias("avg_order_value")
    ).orderBy(F.desc("total_revenue"))

    rev_state.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/revenue_by_state.parquet")

    print("Membuat installment_analysis...")
    inst_analysis = fact_delivered.groupBy("max_installments").agg(
        F.avg("total_payment").alias("avg_order_value"),
        F.count("order_id").alias("total_orders"),
        F.sum("total_payment").alias("total_revenue")
    ).orderBy("max_installments")

    inst_analysis.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/installment_analysis.parquet")

    print("Membuat payment_method_analysis...")
    pay_method = fact_delivered.groupBy("payment_type").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_payment").alias("total_revenue"),
        F.avg("total_payment").alias("avg_order_value"),
        F.avg("max_installments").alias("avg_installments")
    ).orderBy(F.desc("total_revenue"))

    pay_method.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/payment_method_analysis.parquet")

    print("Membuat revenue_leakage...")
    leakage = fact.filter(
        F.col("order_status").isin(["canceled", "unavailable"])
    ).withColumn(
        "leakage_type",
        F.when(F.col("order_status") == "canceled", "Canceled Order")
         .otherwise("Unavailable Order")
    ).select(
        "order_id", "order_status", "payment_type",
        "customer_state", "total_payment", "leakage_type",
        "order_purchase_timestamp"
    )

    leakage.write.mode("overwrite").parquet(f"file://{OUTPUT_DIR}/revenue_leakage.parquet")
    print(f"  revenue_leakage: {leakage.count():,} baris")

    try:
        products = spark.read.parquet(f"file://{LAKE_DIR}/products.parquet")

        try:
            categories = spark.read.parquet(f"file://{LAKE_DIR}/categories.parquet")
            has_categories = True
        except Exception:
            has_categories = False

        print("Membuat order_items_products...")
        items_enriched = items \
            .join(products.select("product_id", "product_category_name"), "product_id", "left")

        if has_categories:
            items_enriched = items_enriched \
                .join(categories, "product_category_name", "left") \
                .select(
                    "order_id", "product_id", "seller_id",
                    "price", "freight_value",
                    F.coalesce(
                        F.col("product_category_name_english"),
                        F.col("product_category_name")
                    ).alias("category")
                )
        else:
            items_enriched = items_enriched.select(
                "order_id", "product_id", "seller_id",
                "price", "freight_value",
                F.col("product_category_name").alias("category")
            )

        items_enriched.write.mode("overwrite").parquet(
            f"file://{OUTPUT_DIR}/order_items_products.parquet"
        )
        print(f"  order_items_products: {items_enriched.count():,} baris")
    except Exception as e:
        print(f"  Skip order_items_products: {e}")

    try:
        sellers = spark.read.parquet(f"file://{LAKE_DIR}/sellers.parquet")

        print("Membuat seller_orders...")
        seller_orders = items.select("order_id", "seller_id") \
            .join(orders.select("order_id", "order_status"), "order_id") \
            .join(sellers.select("seller_id", "seller_state"), "seller_id") \
            .groupBy("seller_id", "seller_state").agg(
                F.count("order_id").alias("total_orders"),
                F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders")
            ).withColumn(
                "cancel_rate_pct",
                F.round(F.col("canceled_orders") * 100.0 / F.col("total_orders"), 2)
            ).orderBy(F.desc("cancel_rate_pct"))

        seller_orders.write.mode("overwrite").parquet(
            f"file://{OUTPUT_DIR}/seller_orders.parquet"
        )
        print(f"  seller_orders: {seller_orders.count():,} baris")
    except Exception as e:
        print(f"  Skip seller_orders: {e}")

    spark.stop()
    print("\nTransformasi selesai!")


if __name__ == "__main__":
    run_transform()
