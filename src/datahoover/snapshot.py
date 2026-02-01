from __future__ import annotations

import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def _iter_snapshot_paths(data_dir: Path, db_path: Path) -> Iterable[Path]:
    for rel in ("raw", "state"):
        path = data_dir / rel
        if path.exists():
            yield path
    if db_path.exists():
        yield db_path


def snapshot_zip(*, data_dir: Path, db_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in _iter_snapshot_paths(data_dir, db_path):
            if path.is_dir():
                for child in path.rglob("*"):
                    if child.is_file():
                        zf.write(child, child.relative_to(data_dir))
            else:
                zf.write(path, path.relative_to(data_dir))
    return output_path


def snapshot_parquet(*, db_path: Path, output_dir: Path) -> Path:
    import duckdb

    if not db_path.exists():
        raise FileNotFoundError(f"Missing DuckDB database: {db_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        for table in tables:
            out_path = output_dir / f"{table}.parquet"
            con.execute(f'COPY "{table}" TO ? (FORMAT PARQUET)', [str(out_path)])
    finally:
        con.close()
    return output_dir


def default_snapshot_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
