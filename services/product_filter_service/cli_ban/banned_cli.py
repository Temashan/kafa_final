import argparse
import json

from services.ingestion_service.config.producer_product_config import KAFKA_CONFIG
from services.product_filter_service.cli_ban.manage_banned_products_use_case import ManageBannedProductsUseCase
from services.product_filter_service.cli_ban.producer_banned_product import KafkaBannedProducer
from services.product_filter_service.config import BANNED_PRODUCT_SCHEMA_PATH


def handle_schema():
    print("\n=== BANNED MESSAGE FORMAT ===\n")
    print(json.dumps({"banned": True, "name": "optional string", "reason": "optional string"},
                     indent=2, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage banned products.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("ban", aliases=["add"])
    add_parser.add_argument("product_id")
    add_parser.add_argument("--name")
    add_parser.add_argument("--reason")

    remove_parser = subparsers.add_parser("unban", aliases=["remove", "rm"])
    remove_parser.add_argument("product_id")

    return parser


def build_use_case() -> ManageBannedProductsUseCase:
    producer = KafkaBannedProducer(
        config=KAFKA_CONFIG,
        topic="products.banned",
        schema_registry_url="http://localhost:8081",
        schema_path=BANNED_PRODUCT_SCHEMA_PATH,
    )
    return ManageBannedProductsUseCase(producer)


def main():
    handle_schema()
    parser = build_parser()
    args = parser.parse_args()
    use_case = build_use_case()

    if args.command in {"ban", "add"}:
        use_case.ban(product_id=args.product_id, name=args.name, reason=args.reason, )
        print(f"[OK] banned {args.product_id}")

    elif args.command in {"unban", "remove", "rm"}:
        use_case.unban(product_id=args.product_id)
        print(f"[OK] unbanned {args.product_id}")

    use_case.flush()


if __name__ == "__main__":
    main()
