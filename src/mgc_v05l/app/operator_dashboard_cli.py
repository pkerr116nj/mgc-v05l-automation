"""Lightweight CLI entrypoint for the local operator dashboard server."""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-v05l-operator-dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--info-file", default=None)
    parser.add_argument("--allow-port-fallback", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    from .operator_dashboard import run_operator_dashboard_server

    run_operator_dashboard_server(
        host=args.host,
        port=args.port,
        info_file=args.info_file,
        allow_port_fallback=bool(args.allow_port_fallback),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
