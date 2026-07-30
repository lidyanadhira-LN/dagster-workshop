import pandas as pd

from main import build_order_features

ORDERS = pd.DataFrame(
    [
        {"order_id": 100, "customer_id": 5, "product_id": 1, "quantity": 2, "order_date": "2026-01-01"},
        {"order_id": 101, "customer_id": 6, "product_id": 2, "quantity": 1, "order_date": "2026-01-02"},
    ]
)
PRODUCTS = pd.DataFrame(
    [
        {"product_id": 1, "name": "Widget", "category": "tools", "price": 9.99},
        {"product_id": 2, "name": "Gadget", "category": "tools", "price": 4.99},
    ]
)


def test_build_order_features_joins_and_computes_total_and_label():
    result = build_order_features(ORDERS, PRODUCTS)

    assert set(result.columns) == {
        "order_id",
        "product_id",
        "customer_id",
        "quantity",
        "price",
        "category",
        "total",
        "is_high_value",
    }
    row_100 = result.loc[result["order_id"] == 100].iloc[0]
    assert row_100["total"] == 19.98
    # median(total) of [19.98, 4.99] is above 4.99, so only the pricier
    # order (100) is above the median and labeled high value.
    assert row_100["is_high_value"] == 1
    row_101 = result.loc[result["order_id"] == 101].iloc[0]
    assert row_101["is_high_value"] == 0
