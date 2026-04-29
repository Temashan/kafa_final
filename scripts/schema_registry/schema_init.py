#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def get_env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw_value}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {raw_value}")
    return value


SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
SCHEMA_SUBJECT = os.getenv("SCHEMA_SUBJECT", "products.raw-value")
SCHEMA_FILE = Path(os.getenv("SCHEMA_FILE", "/schemas/product.avsc"))
SCHEMA_COMPATIBILITY = os.getenv("SCHEMA_COMPATIBILITY", "BACKWARD")
SCHEMA_WAIT_RETRIES = get_env_int("SCHEMA_WAIT_RETRIES", 30)
SCHEMA_WAIT_INTERVAL_SECONDS = get_env_int("SCHEMA_WAIT_INTERVAL_SECONDS", 2)

BANNED_SCHEMA_SUBJECT = os.getenv("BANNED_SCHEMA_SUBJECT", "products.banned-value")
BANNED_SCHEMA_FILE = Path(os.getenv("BANNED_SCHEMA_FILE", "/schemas/banned_product.avsc"))
BANNED_SCHEMA_COMPATIBILITY = os.getenv("BANNED_SCHEMA_COMPATIBILITY", "BACKWARD")


def wait_for_registry() -> bool:
    print("Waiting for Schema Registry...")
    check_url = f"{SCHEMA_REGISTRY_URL}/subjects"
    for _ in range(SCHEMA_WAIT_RETRIES):
        try:
            with urllib.request.urlopen(check_url) as response:
                if response.getcode() == 200:
                    print("Schema Registry is ready")
                    return True
        except Exception:
            print("Schema Registry not ready yet")
            pass
        time.sleep(SCHEMA_WAIT_INTERVAL_SECONDS)
    return False


def _request(url: str, *, data: bytes | None = None, method: str | None = None, ) -> str:
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{exc} | response body: {body}") from exc


def register_schema(schema_content: str, subject: str) -> None:
    print(f"Registering schema for {subject}...")
    schema_json = json.loads(schema_content)
    payload = json.dumps({"schema": json.dumps(schema_json)}).encode("utf-8")
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    response = _request(url, data=payload)
    print(f"Registered: {response}")


def set_compatibility(subject: str, compatibility: str) -> None:
    print(f"Setting compatibility for {subject}...")
    payload = json.dumps({"compatibility": compatibility}).encode("utf-8")
    url = f"{SCHEMA_REGISTRY_URL}/config/{subject}"
    response = _request(url, data=payload, method="PUT")
    print(f"Compatibility set: {response}")


def init_schema(subject: str, file_path: Path, compatibility: str) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Schema file not found: {file_path}")
    schema_content = file_path.read_text(encoding="utf-8")
    register_schema(schema_content, subject)
    set_compatibility(subject, compatibility)


def main() -> int:
    if not wait_for_registry():
        print("Schema Registry not ready")
        return 1
    try:
        init_schema(SCHEMA_SUBJECT, SCHEMA_FILE, SCHEMA_COMPATIBILITY, )
        init_schema(BANNED_SCHEMA_SUBJECT, BANNED_SCHEMA_FILE, BANNED_SCHEMA_COMPATIBILITY, )
        return 0
    except Exception as exc:
        print(f"schema-init failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
