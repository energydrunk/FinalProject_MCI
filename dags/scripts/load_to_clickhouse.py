import pandas as pd
from clickhouse_driver import Client
import os

OUTPUT_DIR = "/opt/airflow/data_lake/transformed"

client = Client(host="clickhouse-server", user="admin", password="rahasia")


def init_database():
    client.execute("CREATE DATABASE IF NOT EXISTS analytics")
    print("Database 'analytics' siap.")


def load_table(parquet_path, table_name, create_sql, columns, truncate=True):
    if not os.path.exists(parquet_path):
        print(f"  SKIP {table_name}: file tidak ditemukan")
        return
    df = pd.read_parquet(parquet_path)
    df = df[columns]
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].fillna('')
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].fillna(0)
    client.execute(create_sql)
    if truncate:
        client.execute(f"TRUNCATE TABLE analytics.{table_name}")
    data = [tuple(row) for row in df.itertuples(index=False)]
    if data:
        client.execute(f"INSERT INTO analytics.{table_name} VALUES", data)
    print(f"  {table_name}: {len(data):,} baris dimuat")


def run_load():
    init_database()

    load_table(
        f"{OUTPUT_DIR}/fact_payments.parquet",
        "fact_payments",
        """
        CREATE TABLE IF NOT EXISTS analytics.fact_payments (
            order_id                 String,
            customer_id              String,
            order_status             String,
            order_purchase_timestamp String,
            total_payment            Float64,
            max_installments         Int32,
            payment_type             String,
            customer_unique_id       String,
            customer_state           String
        ) ENGINE = MergeTree()
        ORDER BY order_id
        """,
        columns=["order_id", "customer_id", "order_status",
                 "order_purchase_timestamp", "total_payment",
                 "max_installments", "payment_type",
                 "customer_unique_id", "customer_state"]
    )

    load_table(
        f"{OUTPUT_DIR}/customer_segments.parquet",
        "customer_segments",
        """
        CREATE TABLE IF NOT EXISTS analytics.customer_segments (
            customer_id        String,
            customer_unique_id String,
            customer_state     String,
            total_spent        Float64,
            order_count        Int32,
            segment            String
        ) ENGINE = MergeTree()
        ORDER BY total_spent
        """,
        columns=["customer_id", "customer_unique_id", "customer_state",
                 "total_spent", "order_count", "segment"]
    )

    load_table(
        f"{OUTPUT_DIR}/revenue_by_state.parquet",
        "revenue_by_state",
        """
        CREATE TABLE IF NOT EXISTS analytics.revenue_by_state (
            customer_state   String,
            total_revenue    Float64,
            total_orders     Int64,
            unique_customers Int64,
            avg_order_value  Float64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
        """,
        columns=["customer_state", "total_revenue", "total_orders",
                 "unique_customers", "avg_order_value"]
    )

    load_table(
        f"{OUTPUT_DIR}/installment_analysis.parquet",
        "installment_analysis",
        """
        CREATE TABLE IF NOT EXISTS analytics.installment_analysis (
            max_installments Int32,
            avg_order_value  Float64,
            total_orders     Int64,
            total_revenue    Float64
        ) ENGINE = MergeTree()
        ORDER BY max_installments
        """,
        columns=["max_installments", "avg_order_value", "total_orders", "total_revenue"]
    )

    load_table(
        f"{OUTPUT_DIR}/payment_method_analysis.parquet",
        "payment_method_analysis",
        """
        CREATE TABLE IF NOT EXISTS analytics.payment_method_analysis (
            payment_type     String,
            total_orders     Int64,
            total_revenue    Float64,
            avg_order_value  Float64,
            avg_installments Float64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
        """,
        columns=["payment_type", "total_orders", "total_revenue",
                 "avg_order_value", "avg_installments"]
    )

    load_table(
        f"{OUTPUT_DIR}/revenue_leakage.parquet",
        "revenue_leakage",
        """
        CREATE TABLE IF NOT EXISTS analytics.revenue_leakage (
            order_id                 String,
            order_status             String,
            payment_type             String,
            customer_state           String,
            total_payment            Float64,
            leakage_type             String,
            order_purchase_timestamp String
        ) ENGINE = MergeTree()
        ORDER BY order_id
        """,
        columns=["order_id", "order_status", "payment_type", "customer_state",
                 "total_payment", "leakage_type", "order_purchase_timestamp"]
    )

    load_table(
        f"{OUTPUT_DIR}/order_items_products.parquet",
        "order_items_products",
        """
        CREATE TABLE IF NOT EXISTS analytics.order_items_products (
            order_id      String,
            product_id    String,
            seller_id     String,
            price         Float64,
            freight_value Float64,
            category      String
        ) ENGINE = MergeTree()
        ORDER BY order_id
        """,
        columns=["order_id", "product_id", "seller_id",
                 "price", "freight_value", "category"]
    )

    load_table(
        f"{OUTPUT_DIR}/seller_orders.parquet",
        "seller_orders",
        """
        CREATE TABLE IF NOT EXISTS analytics.seller_orders (
            seller_id       String,
            seller_state    String,
            total_orders    Int64,
            canceled_orders Int64,
            cancel_rate_pct Float64
        ) ENGINE = MergeTree()
        ORDER BY cancel_rate_pct
        """,
        columns=["seller_id", "seller_state", "total_orders",
                 "canceled_orders", "cancel_rate_pct"]
    )

    print("\nSemua tabel berhasil dimuat ke ClickHouse!")
    print("Buka Metabase di http://localhost:3000 untuk membuat dashboard.")


if __name__ == "__main__":
    run_load()
