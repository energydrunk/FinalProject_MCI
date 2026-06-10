import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"
LAKE_DIR = "/opt/airflow/data_lake"


def load_csv(filename):
    path = f"{DATA_DIR}/{filename}"
    if os.path.exists(path):
        df = pd.read_csv(path)
        print(f"  {filename}: {len(df):,} baris")
        return df
    else:
        print(f"  {filename}: tidak ditemukan, skip.")
        return None


def extract_and_validate():
    os.makedirs(LAKE_DIR, exist_ok=True)

    print("Membaca CSV...")
    orders      = load_csv("orders.csv")
    payments    = load_csv("order_payments.csv")
    customers   = load_csv("customers.csv")
    items       = load_csv("order_items.csv")
    sellers     = load_csv("sellers.csv")
    products    = load_csv("products.csv")
    categories  = load_csv("category_translation.csv")

    print(f"\nDistribusi order status:")
    for status, count in orders["order_status"].value_counts().items():
        print(f"  {status}: {count:,}")

    orders.to_parquet(f"{LAKE_DIR}/orders.parquet", index=False)
    print(f"\nTotal orders disimpan: {len(orders):,}")

    payments_clean = payments[payments["payment_type"] != "not_defined"].copy()
    payments_clean.to_parquet(f"{LAKE_DIR}/payments.parquet", index=False)
    print(f"Payments valid: {len(payments_clean):,} dari {len(payments):,}")

    customers.to_parquet(f"{LAKE_DIR}/customers.parquet", index=False)

    items.to_parquet(f"{LAKE_DIR}/items.parquet", index=False)

    if sellers is not None:
        sellers.to_parquet(f"{LAKE_DIR}/sellers.parquet", index=False)

    if products is not None:
        products.to_parquet(f"{LAKE_DIR}/products.parquet", index=False)

    if categories is not None:
        categories.to_parquet(f"{LAKE_DIR}/categories.parquet", index=False)

    print("\nSemua file tersimpan ke data_lake/")


if __name__ == "__main__":
    extract_and_validate()
