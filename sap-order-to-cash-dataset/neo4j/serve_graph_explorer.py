from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_PATH = SCRIPT_DIR / "explorer" / "data" / "graph_data.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the FastAPI SAP O2C graph explorer.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on.")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload while editing the FastAPI app.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not DEFAULT_DATA_PATH.exists():
        raise SystemExit(
            "Explorer data file is missing. Run `python sap-order-to-cash-dataset/neo4j/build_o2c_graph.py` first."
        )

    print(f"Serving graph explorer at http://{args.host}:{args.port}/")
    print(f"FastAPI app directory: {SCRIPT_DIR}")
    print(f"Graph data: {DEFAULT_DATA_PATH}")
    print("Press Ctrl+C to stop.")

    uvicorn.run(
        "fastapi_graph_explorer:app",
        app_dir=str(SCRIPT_DIR),
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
