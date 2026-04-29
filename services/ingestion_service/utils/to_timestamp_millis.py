from datetime import datetime, timezone


def to_timestamp_millis(value, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a timestamp, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        dt_value = value
    elif isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            dt_value = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"{field_name} has invalid ISO datetime value: {value}") from exc
    else:
        raise ValueError(f"{field_name} has unsupported type: {type(value).__name__}")

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return int(dt_value.timestamp() * 1000)
