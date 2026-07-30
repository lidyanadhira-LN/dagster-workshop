import pandas as pd

from main import ACCURACY_THRESHOLD, model_quality_check, train_classifier

# Perfectly separable on quantity/price so the classifier's accuracy is
# deterministic regardless of the train/test split.
SEPARABLE_FEATURES = pd.DataFrame(
    {
        "order_id": range(1, 11),
        "product_id": [1] * 10,
        "customer_id": [1] * 10,
        "quantity": [1, 1, 1, 1, 1, 10, 10, 10, 10, 10],
        "price": [5.0, 5.0, 5.0, 5.0, 5.0, 50.0, 50.0, 50.0, 50.0, 50.0],
        "category": ["a"] * 10,
        "total": [5.0] * 5 + [500.0] * 5,
        "is_high_value": [0] * 5 + [1] * 5,
    }
)


def test_train_classifier_meets_accuracy_threshold_on_separable_data():
    bundle = train_classifier(SEPARABLE_FEATURES)

    assert bundle["accuracy"] >= ACCURACY_THRESHOLD
    assert bundle["feature_columns"] == ["quantity", "price", "category_a"]
    assert hasattr(bundle["model"], "predict")


def test_model_quality_check_fails_below_accuracy_threshold():
    low_accuracy_bundle = {"model": None, "accuracy": 0.3, "feature_columns": []}

    result = model_quality_check(low_accuracy_bundle)

    assert result.passed is False
