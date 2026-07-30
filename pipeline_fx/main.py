import pandas as pd
from dagster import Definitions, ScheduleDefinition, asset, define_asset_job

import db
import source


@asset
def raw_exchange_rates() -> pd.DataFrame:
    payload = source.fetch_latest_rates(base="USD")
    rows = [
        {"base_currency": "USD", "quote_currency": currency, "rate": rate}
        for currency, rate in payload["rates"].items()
    ]
    return pd.DataFrame(rows)


@asset
def exchange_rates_table(raw_exchange_rates: pd.DataFrame) -> int:
    return db.load_table(raw_exchange_rates, "exchange_rates")


# TODO(exercise-2): add an `orders_in_eur` asset that reads the `orders` and
# `products` tables written by pipeline_products, joins them with
# exchange_rates_table, and converts order totals to EUR.
@asset
def orders_in_eur() -> pd.DataFrame:
    engine = db.get_engine()
    
    # 1. Baca tabel dari warehouse Postgres
    orders_df = pd.read_sql("SELECT * FROM orders", con=engine)
    products_df = pd.read_sql("SELECT * FROM products", con=engine)
    rates_df = pd.read_sql("SELECT * FROM exchange_rates", con=engine)
    
    # 2. Gabungkan orders dan products untuk hitung total harga awal (USD)
    merged = orders_df.merge(products_df, on="product_id")
    merged["total_usd"] = merged["quantity"] * merged["price"]
    
    # 3. Cari kurs EUR dari tabel exchange_rates
    eur_rate_row = rates_df[rates_df["quote_currency"] == "EUR"]
    
    if not eur_rate_row.empty:
        eur_rate = float(eur_rate_row["rate"].values[0])
    else:
        eur_rate = 1.0  # Fallback/default jika rate tidak ditemukan
        
    # 4. Konversi total harga ke EUR
    merged["total_eur"] = merged["total_usd"] * eur_rate
    
    # 5. Pilih kolom yang relevan dan simpan/kembalikan hasil
    result_df = merged[["order_id", "customer_id", "product_id", "quantity", "total_usd", "total_eur"]]
    
    # Simpan kembali ke database warehouse
    result_df.to_sql("orders_in_eur", con=engine, if_exists="replace", index=False)
    
    return result_df


refresh_fx_job = define_asset_job(name="refresh_fx_job")

refresh_fx_daily = ScheduleDefinition(
    name="refresh_fx_daily",
    job=refresh_fx_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[
        raw_exchange_rates,
        exchange_rates_table,
        orders_in_eur,  # <--- Didaftarkan di sini
    ],
    jobs=[refresh_fx_job],
    schedules=[refresh_fx_daily],
)