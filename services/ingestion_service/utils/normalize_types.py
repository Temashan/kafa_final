from decimal import Decimal


def normalize_types(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: normalize_types(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [normalize_types(item) for item in value]
    return value