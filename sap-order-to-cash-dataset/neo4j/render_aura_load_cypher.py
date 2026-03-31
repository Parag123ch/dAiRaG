from __future__ import annotations

import argparse
import re
from pathlib import Path
from urllib.parse import quote, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / 'load_o2c_graph.cypher'
DEFAULT_OUTPUT = SCRIPT_DIR / 'load_o2c_graph_aura.cypher'
FILE_URL_PATTERN = re.compile(r"'file:///(?P<filename>[^']+)'")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Render an Aura-compatible LOAD CSV script by replacing file:/// URLs with a remote base URL.'
    )
    parser.add_argument(
        '--base-url',
        required=True,
        help='Remote HTTPS base URL that exposes the Neo4j import CSV files, for example https://storage.example.com/o2c-import',
    )
    parser.add_argument(
        '--input',
        type=Path,
        default=DEFAULT_INPUT,
        help=f'Input Cypher file to transform. Defaults to {DEFAULT_INPUT}',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f'Output Cypher file to write. Defaults to {DEFAULT_OUTPUT}',
    )
    return parser.parse_args()


def normalize_base_url(value: str) -> str:
    base_url = value.strip().rstrip('/')
    parsed = urlparse(base_url)
    if parsed.scheme not in {'https', 'http'}:
        raise SystemExit('The Aura LOAD CSV base URL must start with http:// or https://')
    if not parsed.netloc:
        raise SystemExit('The Aura LOAD CSV base URL must include a host name')
    return base_url


def replace_file_urls(source: str, base_url: str) -> tuple[str, list[str]]:
    seen: list[str] = []

    def replacer(match: re.Match[str]) -> str:
        filename = match.group('filename')
        seen.append(filename)
        return f"'{base_url}/{quote(filename)}'"

    rendered = FILE_URL_PATTERN.sub(replacer, source)
    return rendered, seen


def main() -> None:
    args = parse_args()
    base_url = normalize_base_url(args.base_url)

    if not args.input.exists():
        raise SystemExit(f'Input file not found: {args.input}')

    source = args.input.read_text(encoding='utf-8')
    rendered, filenames = replace_file_urls(source, base_url)
    if not filenames:
        raise SystemExit('No file:/// CSV references were found in the input script.')

    header = '\n'.join(
        [
            '// Aura-compatible LOAD CSV script',
            f'// Generated from: {args.input.name}',
            f'// Remote CSV base URL: {base_url}',
            '// Note: Aura cannot read local file:/// CSV paths, so each CSV must be available at a remote URL.',
            '',
        ]
    )
    args.output.write_text(header + rendered, encoding='utf-8')

    print(f'Wrote {args.output}')
    print('CSV files referenced:')
    for filename in filenames:
        print(f'- {filename}')


if __name__ == '__main__':
    main()
