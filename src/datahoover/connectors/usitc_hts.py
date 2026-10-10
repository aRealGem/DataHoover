"""USITC Harmonized Tariff Schedule: versioned edition snapshots + diff (DH-PULLS-001).

Bounded by design: only the latest edition and the one immediately before it
(``max_editions``, default 2) are fetched. No historical bulk.

Per edition:
  * metadata parsed from USITC's archive list page, as USITC states it
    - ``archive_published_date``: the date shown in the archive heading
    - ``release_date``: the ``releaseDate`` USITC puts in the edition's HTML link
    These differ (e.g. Rev 21: published 2026-10-09, releaseDate 2026-10-07)
    and NEITHER is a legal effective date. ``effective_date`` stays NULL with
    ``effective_date_status = 'unknown_not_stated_by_source'``.
  * ``modification_sources``: the legal instruments USITC lists for the edition,
    with any Federal Register document number found in their links -- the
    route to the instrument's own effective date, kept as a link, not copied.
  * the edition's JSON file stored once, immutably, by sha256; conditional GET
    (ETag / Last-Modified) means an unchanged edition costs no download.

A silent USITC re-post of the same edition with different bytes is kept as a
second snapshot of that edition, never an overwrite.
"""
from __future__ import annotations

import html as htmllib
import json
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import duckdb
import httpx

from ..sources import load_sources
from ._provenance import USER_AGENT, content_hash, sha256_bytes, utc_now, write_immutable
from ._retry import fetch_with_retry

BASE = "https://www.usitc.gov"
EFFECTIVE_UNKNOWN = "unknown_not_stated_by_source"
LINE_FIELDS = ["htsno", "indent", "description", "units", "general", "special", "other",
               "footnotes", "quotaQuantity", "additionalDuties"]

# (url, extra_headers) -> (status, headers, body). 304 returns body b"".
HttpGet = Callable[[str, Dict[str, str]], Tuple[int, Dict[str, str], bytes]]


def _default_http_get(timeout_s: float = 120.0) -> HttpGet:
    def _get(url: str, extra: Dict[str, str]) -> Tuple[int, Dict[str, str], bytes]:
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            r = client.get(url, headers={"User-Agent": USER_AGENT, **extra})
        if r.status_code == 304:
            return 304, dict(r.headers), b""
        r.raise_for_status()
        return r.status_code, dict(r.headers), r.content
    return _get


_EDITION_RE = re.compile(r"(\d{4})HTS([A-Za-z]*?)(?:Rev(\d+))?$")


def edition_sort_key(name: str) -> Tuple[int, int, int]:
    """(year, revision, parsed?) -- Basic edition is revision 0. Unparseable sorts last."""
    m = _EDITION_RE.match(name or "")
    if not m:
        return (0, 0, 0)
    year, _, rev = m.groups()
    return (int(year), int(rev) if rev else 0, 1)


def _parse_us_date(text: str) -> Optional[date]:
    for fmt in ("%B %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _strip(fragment: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def parse_archive_page(page: str) -> List[Dict[str, Any]]:
    """Parse USITC's HTS archive list into edition dicts (page order preserved)."""
    out: List[Dict[str, Any]] = []
    heads = list(re.finditer(r'views-field-field-hts-arch-published-date[^>]*>\s*<span[^>]*>(.*?)</span>', page, re.S))
    for idx, h in enumerate(heads):
        end = heads[idx + 1].start() if idx + 1 < len(heads) else len(page)
        block = page[h.end():end]
        label_full = _strip(h.group(1))
        m = re.match(r"^(.*?)\s*\(([^)]*)\)\s*$", label_full)
        label, pub_raw = (m.group(1), m.group(2)) if m else (label_full, None)
        hrefs = [htmllib.unescape(x) for x in re.findall(r'href="([^"]+)"', block)]
        dl = next((x for x in hrefs if "hts.usitc.gov/download" in x), None)
        name = rel_raw = None
        if dl:
            q = parse_qs(urlparse(dl).query)
            name = (q.get("release") or [None])[0]
            rel_raw = (q.get("releaseDate") or [None])[0]
        json_url = next((urljoin(BASE, x) for x in hrefs if x.lower().endswith(".json")), None)
        mods: List[Dict[str, Any]] = []
        mi = block.find("Modification Source")
        if mi >= 0:
            mblock = block[mi:]
            for a in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', mblock, re.S):
                url = htmllib.unescape(a.group(1))
                if "sprite.svg" in url:
                    continue
                fr = re.search(r"federalregister\.gov/documents/\d{4}/\d{2}/\d{2}/([0-9]{4}-[0-9]+)/", url)
                mods.append({"text": _strip(a.group(2)), "url": url,
                             "fr_document_number": fr.group(1) if fr else None})
            # Anchor text alone drops the citation USITC prints beside it
            # ("( 91 Fed. Reg. 60360 )"), so the whole block is kept verbatim too.
            raw_text = _strip(mblock).replace("Modification Source(s):", "", 1).strip()
            mods = [{"items": mods, "raw_text": raw_text[:4000]}]
        out.append({
            "edition_name": name, "edition_label": label,
            "archive_published_date_raw": pub_raw,
            "archive_published_date": _parse_us_date(pub_raw) if pub_raw else None,
            "release_date_raw": rel_raw,
            "release_date": _parse_us_date(rel_raw) if rel_raw else None,
            "json_url": json_url, "modification_sources": mods[0] if mods else None,
        })
    return out


def select_editions(editions: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    usable = [e for e in editions if e["edition_name"] and e["json_url"]]
    usable.sort(key=lambda e: edition_sort_key(e["edition_name"]), reverse=True)
    return usable[:n]


def _line_keys(lines: List[Dict[str, Any]]) -> List[str]:
    """Stable identity per line. Rows without an HTS number (headings) key on
    the nearest preceding HTS number + indent + description; repeats get #n."""
    keys, seen, anchor = [], {}, ""
    for ln in lines:
        hts = (ln.get("htsno") or "").strip()
        if hts:
            anchor = hts
            base = hts
        else:
            base = f"{anchor}|{ln.get('indent')}|{(ln.get('description') or '').strip()}"
        n = seen.get(base, 0)
        seen[base] = n + 1
        keys.append(base if n == 0 else f"{base}#{n + 1}")
    return keys


def _line_payload(ln: Dict[str, Any]) -> Dict[str, Any]:
    return {f: ln.get(f) for f in LINE_FIELDS}


def normalize_lines(lines: List[Dict[str, Any]], snapshot_sha: str) -> List[Dict[str, Any]]:
    rows = []
    for i, (ln, key) in enumerate(zip(lines, _line_keys(lines))):
        try:
            indent = int(ln.get("indent")) if ln.get("indent") not in (None, "") else None
        except (TypeError, ValueError):
            indent = None
        rows.append({
            "snapshot_sha256": snapshot_sha, "line_no": i, "line_key": key,
            "htsno": ln.get("htsno"), "indent": indent, "description": ln.get("description"),
            "units": json.dumps(ln.get("units"), ensure_ascii=False),
            "general_rate": ln.get("general"), "special_rate": ln.get("special"),
            "other_rate": ln.get("other"),
            "footnotes": json.dumps(ln.get("footnotes"), ensure_ascii=False),
            "additional_duties": None if ln.get("additionalDuties") is None else str(ln.get("additionalDuties")),
            "quota_quantity": None if ln.get("quotaQuantity") is None else str(ln.get("quotaQuantity")),
            "line_sha256": content_hash(_line_payload(ln)),
        })
    return rows


def diff_lines(before: List[Dict[str, Any]], after: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Line-level diff keyed by line identity: added / removed / changed."""
    b = dict(zip(_line_keys(before), before))
    a = dict(zip(_line_keys(after), after))
    out = []
    for k in b.keys() - a.keys():
        out.append({"line_key": k, "change_type": "removed", "htsno": b[k].get("htsno"),
                    "changed_fields": None, "before": _line_payload(b[k]), "after": None})
    for k in a.keys() - b.keys():
        out.append({"line_key": k, "change_type": "added", "htsno": a[k].get("htsno"),
                    "changed_fields": None, "before": None, "after": _line_payload(a[k])})
    for k in a.keys() & b.keys():
        pb, pa = _line_payload(b[k]), _line_payload(a[k])
        if pb != pa:
            out.append({"line_key": k, "change_type": "changed", "htsno": a[k].get("htsno"),
                        "changed_fields": sorted(f for f in LINE_FIELDS if pb.get(f) != pa.get(f)),
                        "before": pb, "after": pa})
    out.sort(key=lambda d: (d["line_key"], d["change_type"]))
    return out


def _schema_drift(lines: List[Dict[str, Any]]) -> List[str]:
    known = set(LINE_FIELDS) | {"superior", "addiitionalDuties"}  # sic: USITC's own misspelt key
    seen = set().union(*(ln.keys() for ln in lines)) if lines else set()
    notes = []
    if seen - known:
        notes.append(f"unexpected line fields {sorted(seen - known)}")
    missing = {"htsno", "description"} - seen
    if missing:
        notes.append(f"missing core line fields {sorted(missing)}")
    return notes


def _state_path(data_dir: Path, name: str) -> Path:
    return data_dir / "state" / f"{name}.json"


def ingest_usitc_hts(
    *,
    config_path: Path,
    source_name: str,
    data_dir: Path,
    db_path: Path,
    http_get: Optional[HttpGet] = None,
) -> Dict[str, Any]:
    from ..storage.duckdb_store import init_db, log_run
    from ..storage.policy_store import init_policy_tables, record_raw_response

    sources = load_sources(config_path)
    if source_name not in sources:
        raise SystemExit(f"Unknown source '{source_name}'. Available: {', '.join(sorted(sources))}")
    src = sources[source_name]
    n_editions = int((src.extra or {}).get("max_editions", 2))
    if n_editions > 2:
        raise SystemExit("usitc_hts: max_editions > 2 is a historical pull; not authorized (DH-PULLS-001)")
    get = http_get or _default_http_get()
    raw_dir = data_dir / "raw" / src.name
    state_file = _state_path(data_dir, src.name)
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else {}
    state.setdefault("http", {})

    init_db(db_path)
    init_policy_tables(db_path)
    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    stamp = started.strftime("%Y-%m-%dT%H-%M-%SZ")
    n_req = n_bytes = 0
    drift: List[str] = []
    try:
        status, _, body = fetch_with_retry(lambda: get(src.url, {}))
        n_req += 1
        n_bytes += len(body)
        page_path = raw_dir / "archive_list" / f"{stamp}.html"
        page_sha = write_immutable(page_path, body)
        record_raw_response(db_path, {"raw_sha256": page_sha, "source": src.name, "endpoint": src.url,
                                      "query": "", "fetched_at": utc_now(), "http_status": status,
                                      "n_bytes": len(body), "raw_path": str(page_path)})
        editions = select_editions(parse_archive_page(body.decode("utf-8", errors="replace")), n_editions)
        if len(editions) < n_editions:
            raise RuntimeError(f"archive page yielded {len(editions)} usable editions; expected {n_editions} (page layout drift?)")

        seen_at = utc_now()
        snaps: Dict[str, Tuple[str, List[Dict[str, Any]]]] = {}
        con = duckdb.connect(str(db_path))
        try:
            for ed in editions:
                name, url = ed["edition_name"], ed["json_url"]
                cond = state["http"].get(url, {})
                extra = {}
                if cond.get("etag"):
                    extra["If-None-Match"] = cond["etag"]
                elif cond.get("last_modified"):
                    extra["If-Modified-Since"] = cond["last_modified"]
                st, hdrs, data = fetch_with_retry(lambda: get(url, extra))
                n_req += 1
                n_bytes += len(data)
                if st == 304:
                    sha = cond["sha256"]
                    path = Path(cond["raw_path"])
                    data = path.read_bytes()
                    if sha256_bytes(data) != sha:
                        raise RuntimeError(f"{name}: local snapshot {path} no longer matches recorded sha256")
                else:
                    sha = sha256_bytes(data)
                    path = raw_dir / f"{name}__{sha[:16]}.json"
                    write_immutable(path, data)
                    record_raw_response(db_path, {"raw_sha256": sha, "source": src.name, "endpoint": url,
                                                  "query": "", "fetched_at": utc_now(), "http_status": st,
                                                  "n_bytes": len(data), "raw_path": str(path)})
                    lower = {k.lower(): v for k, v in hdrs.items()}
                    state["http"][url] = {"etag": lower.get("etag"), "last_modified": lower.get("last-modified"),
                                          "sha256": sha, "raw_path": str(path)}
                lines = json.loads(data)
                if not isinstance(lines, list):
                    raise RuntimeError(f"{name}: expected a JSON list of HTS lines, got {type(lines).__name__}")
                drift += [f"{name}: {d}" for d in _schema_drift(lines)]
                snaps[name] = (sha, lines)

                have = con.execute("SELECT 1 FROM hts_snapshots WHERE snapshot_sha256 = ?", [sha]).fetchone()
                if not have:
                    lower = {k.lower(): v for k, v in hdrs.items()}
                    con.execute("INSERT INTO hts_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)",
                                [sha, name, len(data), len(lines), lower.get("last-modified"), str(path), seen_at])
                    rows = normalize_lines(lines, sha)
                    cols = list(rows[0].keys()) if rows else []
                    if rows:
                        con.executemany(
                            f"INSERT INTO hts_lines ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                            [[r[c] for c in cols] for r in rows])
                prev = con.execute("SELECT first_seen_at FROM hts_editions WHERE edition_name = ?", [name]).fetchone()
                con.execute(
                    "INSERT OR REPLACE INTO hts_editions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [name, ed["edition_label"], ed["archive_published_date_raw"], ed["archive_published_date"],
                     ed["release_date_raw"], ed["release_date"], None, EFFECTIVE_UNKNOWN,
                     json.dumps(ed["modification_sources"], ensure_ascii=False) if ed["modification_sources"] else None, url, sha,
                     prev[0] if prev else seen_at, seen_at])

            newer, older = editions[0]["edition_name"], editions[1]["edition_name"] if len(editions) > 1 else None
            n_diff = 0
            if older:
                (sha_o, lines_o), (sha_n, lines_n) = snaps[older], snaps[newer]
                done = con.execute("SELECT COUNT(*) FROM hts_edition_diffs WHERE from_snapshot_sha256 = ? AND to_snapshot_sha256 = ?",
                                   [sha_o, sha_n]).fetchone()[0]
                diffs = diff_lines(lines_o, lines_n)
                n_diff = len(diffs)
                if not done and diffs:
                    now = utc_now()
                    con.executemany(
                        "INSERT INTO hts_edition_diffs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [[sha_o, sha_n, older, newer, d["line_key"], d["change_type"], d["htsno"],
                          json.dumps(d["changed_fields"]) if d["changed_fields"] else None,
                          json.dumps(d["before"], ensure_ascii=False) if d["before"] else None,
                          json.dumps(d["after"], ensure_ascii=False) if d["after"] else None, now]
                         for d in diffs])
        finally:
            con.close()

        state.update({"last_success_at": started.isoformat(), "editions": [e["edition_name"] for e in editions],
                      "last_schema_drift": drift})
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state, indent=2, sort_keys=True, default=str), encoding="utf-8")
        msg = (f"editions={[e['edition_name'] for e in editions]} req={n_req} bytes={n_bytes} "
               f"lines={{{', '.join(f'{k}: {len(v[1])}' for k, v in snaps.items())}}} diff_rows={n_diff}"
               + (f" drift={drift}" if drift else ""))
        log_run(db_path, run_id=run_id, source=src.name, feed_url=src.url, started_at=started,
                ended_at=datetime.now(timezone.utc), status="ok" if not drift else "ok_drift",
                n_total=sum(len(v[1]) for v in snaps.values()), n_new=n_diff, message=msg)
        print(f"[{src.name}] {msg}")
        return {"editions": editions, "requests": n_req, "bytes": n_bytes, "diff_rows": n_diff,
                "lines": {k: len(v[1]) for k, v in snaps.items()}, "schema_drift": drift}
    except Exception as e:
        try:
            log_run(db_path, run_id=run_id, source=src.name, feed_url=src.url, started_at=started,
                    ended_at=datetime.now(timezone.utc), status="error", n_total=0, n_new=0, message=str(e))
        except Exception:
            pass
        raise
