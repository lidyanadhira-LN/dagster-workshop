import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, Definitions
import db  # Diubah dari `from . import db` menjadi `import db`

@asset
def cross_pipeline_summary() -> pd.DataFrame:
    engine = db.get_engine()
    
    # Membaca data dari tabel warehouse
    orders = pd.read_sql("SELECT * FROM orders", con=engine)
    predictions = pd.read_sql("SELECT * FROM order_value_predictions", con=engine)
    
    # Gabungkan data transaksi dengan hasil prediksi ML
    summary = orders.merge(predictions, on="order_id", how="inner")
    
    # Simpan kembali hasilnya ke database warehouse
    summary.to_sql("cross_pipeline_summary", con=engine, if_exists="replace", index=False)
    
    return summary

@asset_check(asset=cross_pipeline_summary)
def check_summary_has_rows(cross_pipeline_summary: pd.DataFrame) -> AssetCheckResult:
    passed = len(cross_pipeline_summary) > 0
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": len(cross_pipeline_summary)}
    )

defs = Definitions(
    assets=[cross_pipeline_summary],
    asset_checks=[check_summary_has_rows]
)