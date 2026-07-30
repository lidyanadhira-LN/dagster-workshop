from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
import source
from main import exchange_rates_table, raw_exchange_rates

FAKE_PAYLOAD = {"base": "USD", "rates": {"EUR": 0.9, "GBP": 0.8}}


def test_exchange_rates_pipeline_loads_expected_rows():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(
        source, "fetch_latest_rates", return_value=FAKE_PAYLOAD
    ), patch.object(db, "load_table", side_effect=fake_load_table):
        result = materialize([raw_exchange_rates, exchange_rates_table])

    assert result.success
    table = loaded["exchange_rates"]
    assert set(table["quote_currency"]) == {"EUR", "GBP"}
    assert table.loc[table["quote_currency"] == "EUR", "rate"].iloc[0] == 0.9
