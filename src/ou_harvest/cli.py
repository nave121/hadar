from __future__ import annotations

import argparse
import json

from .config import AppConfig
from .runner import PipelineRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ou_harvest")
    parser.add_argument("--config", default=None, help="Path to ou_harvest.toml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("discover")
    subparsers.add_parser("crawl")
    subparsers.add_parser("parse")
    subparsers.add_parser("demographics")
    subparsers.add_parser("doctor")
    subparsers.add_parser("tui")

    enrich = subparsers.add_parser("enrich")
    enrich.add_argument("--provider", choices=["ollama", "openai"], required=True)

    review = subparsers.add_parser("review")
    review.add_argument("--json", action="store_true", help="Emit review queue to stdout")

    export = subparsers.add_parser("export")
    export.add_argument("--format", choices=["json", "jsonl"], default="json")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = AppConfig.load(args.config)
    runner = PipelineRunner(config)

    if args.command == "discover":
        snapshot = runner.discover()
        print(snapshot.model_dump_json(indent=2))
    elif args.command == "crawl":
        urls = runner.crawl()
        print(json.dumps({"crawled_urls": urls}, ensure_ascii=False, indent=2))
    elif args.command == "parse":
        records = runner.parse()
        print(json.dumps({"records": len(records)}, ensure_ascii=False, indent=2))
    elif args.command == "demographics":
        records = runner.analyze_demographics()
        print(json.dumps({"records": len(records)}, ensure_ascii=False, indent=2))
    elif args.command == "enrich":
        records = runner.enrich(args.provider)
        print(json.dumps({"records": len(records), "provider": args.provider}, ensure_ascii=False, indent=2))
    elif args.command == "review":
        queue = runner.review()
        if args.json:
            print(json.dumps(queue, ensure_ascii=False, indent=2))
        else:
            print(json.dumps({"review_queue": len(queue)}, ensure_ascii=False, indent=2))
    elif args.command == "export":
        path = runner.export(args.format)
        print(json.dumps({"path": str(path)}, ensure_ascii=False, indent=2))
    elif args.command == "doctor":
        print(json.dumps(runner.doctor(), ensure_ascii=False, indent=2))
    elif args.command == "tui":
        try:
            from .tui import OuHarvestTUI
        except ImportError as exc:
            raise RuntimeError("Textual is not installed. Install with `pip install -e .[tui]`.") from exc
        app = OuHarvestTUI(config_path=config.source_path or args.config or "ou_harvest.toml")
        app.run()


if __name__ == "__main__":
    main()
