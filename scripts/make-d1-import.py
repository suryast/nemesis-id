#!/usr/bin/env python3
from pathlib import Path
import argparse

SKIP_PREFIXES = [
    'CREATE TABLE assets',
    'INSERT INTO "assets"',
    'CREATE INDEX idx_assets',
]


def should_skip(line: str) -> bool:
    stripped = line.strip()
    return any(stripped.startswith(prefix) for prefix in SKIP_PREFIXES)


def main() -> None:
    parser = argparse.ArgumentParser(description='Create a D1-friendly SQL import by removing oversized asset rows.')
    parser.add_argument('--in', dest='input_path', required=True)
    parser.add_argument('--out', dest='output_path', required=True)
    args = parser.parse_args()

    source = Path(args.input_path)
    target = Path(args.output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with source.open('r', encoding='utf-8', errors='ignore') as src, target.open('w', encoding='utf-8') as dst:
        for line in src:
            if should_skip(line):
                continue
            dst.write(line)

    print(f'Wrote {target}')
    print('Large assets were removed. Serve geojson as static files instead of D1 rows.')


if __name__ == '__main__':
    main()
