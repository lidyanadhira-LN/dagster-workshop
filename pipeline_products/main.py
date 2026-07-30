import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db
import source


@asset
def raw_products() -> pd.DataFrame:
    products = source.fetch_products()
    return pd.DataFrame(products)[["id", "title", "category", "price"]].rename(
        columns={"id": "product_id", "title": "name"}
    )


@asset
def raw_orders() -> pd.DataFrame:
    carts = source.fetch_carts()
    rows = []
    for cart in carts:
        for item in cart["products"]:
            rows.append(
                {
                    "order_id": cart["id"],
                    "customer_id": cart["userId"],
                    "product_id": item["productId"],
                    "quantity": item["quantity"],
                    "order_date": cart["date"],
                }
            )
    return pd.DataFrame(rows)


@asset
def products_table(raw_products: pd.DataFrame) -> int:
    return db.load_table(raw_products, "products")


@asset
def orders_table(raw_orders: pd.DataFrame) -> int:
    return db.load_table(raw_orders, "orders")


# Exercise 1: top_selling_products asset
@asset
def top_selling_products(raw_orders: pd.DataFrame, raw_products: pd.DataFrame) -> pd.DataFrame:
    orders_df = raw_orders.copy()
    products_df = raw_products.copy()

    # Samakan tipe data product_id agar merge tidak error
    orders_df["product_id"] = orders_df["product_id"].astype(str)
    products_df["product_id"] = products_df["product_id"].astype(str)

    # Join dataset
    merged_df = orders_df.merge(products_df, on="product_id")

    # Ambil kolom nama produk yang sesuai
    name_col = "name" if "name" in merged_df.columns else "title"

    # Hitung Top 5
    top_5_df = (
        merged_df.groupby(["product_id", name_col])["quantity"]
        .sum()
        .reset_index()
        .sort_values(by="quantity", ascending=False)
        .head(5)
    )
    return top_5_df


# Exercise 3: Asset check untuk raw_orders
@asset_check(asset=raw_orders)
def check_raw_orders_quantity_positive(raw_orders: pd.DataFrame) -> AssetCheckResult:
    invalid_rows = raw_orders[raw_orders["quantity"] <= 0]
    return AssetCheckResult(
        passed=len(invalid_rows) == 0,
        metadata={"invalid_records_count": len(invalid_rows)},
    )


refresh_products_job = define_asset_job(name="refresh_products_job")

refresh_products_daily = ScheduleDefinition(
    name="refresh_products_daily",
    job=refresh_products_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[
        raw_products,
        raw_orders,
        products_table,
        orders_table,
        top_selling_products,
    ],
    asset_checks=[
        check_raw_orders_quantity_positive,
    ],
    jobs=[refresh_products_job],
    schedules=[refresh_products_daily],
)