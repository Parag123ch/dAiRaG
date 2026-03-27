from __future__ import annotations

import argparse
from pathlib import Path


def combine_folder(folder: Path, output_dir: Path) -> tuple[Path, int]:
    input_files = sorted(path for path in folder.glob("*.jsonl") if path.is_file())
    if not input_files:
        raise ValueError(f"No .jsonl files found in {folder}")

    output_path = output_dir / f"{folder.name}.jsonl"
    lines_written = 0

    with output_path.open("w", encoding="utf-8", newline="\n") as destination:
        for input_file in input_files:
            with input_file.open("r", encoding="utf-8") as source:
                for line in source:
                    destination.write(line.rstrip("\n"))
                    destination.write("\n")
                    lines_written += 1

    return output_path, lines_written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine JSONL part files from each child folder into one JSONL file per folder."
    )
    parser.add_argument(
        "--input-root",
        default="sap-o2c-data",
        help="Directory containing child folders with JSONL part files.",
    )
    parser.add_argument(
        "--output-dir",
        default="combined",
        help="Directory where combined JSONL files will be written.",
    )
    args = parser.parse_args()

    input_root = Path(args.input_root).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not input_root.exists() or not input_root.is_dir():
        raise SystemExit(f"Input root does not exist or is not a directory: {input_root}")

    output_dir.mkdir(parents=True, exist_ok=True)

    folders = sorted(path for path in input_root.iterdir() if path.is_dir())
    if not folders:
        raise SystemExit(f"No child folders found in input root: {input_root}")

    combined_any = False
    for folder in folders:
        jsonl_files = list(folder.glob("*.jsonl"))
        if not jsonl_files:
            continue

        output_path, lines_written = combine_folder(folder, output_dir)
        print(f"{folder.name}: merged {len(jsonl_files)} files into {output_path} ({lines_written} lines)")
        combined_any = True

    if not combined_any:
        raise SystemExit(f"No JSONL files found under child folders of: {input_root}")


if __name__ == "__main__":
    main()
