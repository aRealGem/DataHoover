"""Re-skin an already-published /datahoover/ rollup index.html in the LCARS console look (ISC-057).

Usage: python3 -I scripts/reskin_rollup_index.py LIVE_INDEX.html OUT.html

Visual only. Reads the served rollup (h1, then h2 + ul/li sections as written by
publish_sentiment_to_expressionpi.py), keeps every heading, list item and href verbatim,
and refuses (exit 2) if the page holds anything else, so hand edits are never dropped.
"""
from __future__ import annotations

import html
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lcars_page  # noqa: E402

ALLOWED = {"html", "head", "meta", "title", "body", "h1", "h2", "ul", "li", "a", "style"}


class _Rollup(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.stack: list[str] = []
        self.title = ""
        self.h1 = ""
        self.sections: list[list] = []  # [h2_text, [li_inner_html, ...]]
        self.bad: list[str] = []
        self._li: list[str] | None = None
        self._text_target: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag not in ALLOWED:
            self.bad.append(tag)
        if self._li is not None:
            a = "".join(f' {k}="{html.escape(v or "", quote=True)}"' for k, v in attrs)
            self._li.append(f"<{tag}{a}>")
        if tag == "li":
            self._li = []
        elif tag in ("h1", "h2", "title"):
            self._text_target = tag
            if tag == "h2":
                self.sections.append(["", []])
        self.stack.append(tag)

    def handle_endtag(self, tag):
        if tag == "li" and self._li is not None:
            self.sections[-1][1].append("".join(self._li).strip())
            self._li = None
        elif self._li is not None:
            self._li.append(f"</{tag}>")
        if tag in ("h1", "h2", "title"):
            self._text_target = None
        if self.stack:
            self.stack.pop()

    def handle_data(self, data):
        if self._li is not None:
            self._li.append(data)
        elif self._text_target == "title":
            self.title += data
        elif self._text_target == "h1":
            self.h1 += data
        elif self._text_target == "h2":
            self.sections[-1][0] += data
        elif data.strip() and (not self.stack or self.stack[-1] not in ("style", "head")):
            self.bad.append("text:" + data.strip()[:40])

    def handle_entityref(self, name):
        self.handle_data(f"&{name};")

    def handle_charref(self, name):
        self.handle_data(f"&#{name};")


def reskin(src: str) -> str:
    p = _Rollup()
    p.feed(src)
    if p.bad or not p.h1 or not p.sections or any(not s[1] for s in p.sections):
        raise ValueError(f"unexpected rollup structure: {p.bad[:5]}")
    side, blocks = [], []
    for i, (h2, items) in enumerate(p.sections):
        sid = f"s{i + 1}"
        side.append((html.unescape(h2).strip(), f"#{sid}"))
        lis = "\n    ".join(f"<li>{x}</li>" for x in items)
        cls = "lc-panel alt" if i % 2 else "lc-panel"
        # Day strip from the day-folder links (./YYYY-MM-DD/) already in the list; nothing new is asserted.
        days = sorted({m.group(1) for x in items for m in re.finditer(r'href="\./(\d{4}-\d{2}-\d{2})/"', x)}, reverse=True)
        strip = ("\n  " + lcars_page.date_strip(days)) if days else ""
        blocks.append(f'  <section class="{cls}" id="{sid}"><h2>{h2.strip()}</h2>{strip}\n  <ul class="lc-list">\n    {lis}\n  </ul></section>')
    side.append(("Dashboards", "https://myfreakingstuff.com/doku.php?id=me:dashboards"))
    main = f"  <h1>{p.h1.strip()}</h1>\n\n" + "\n\n".join(blocks)
    return lcars_page.page(html.unescape(p.title.strip()) or "DataHoover", "DataHoover", "Reports · published PDFs", side, main)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        out = reskin(Path(argv[1]).read_text(encoding="utf-8"))
    except ValueError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    Path(argv[2]).write_text(out, encoding="utf-8")
    print(f"reskinned bytes={len(out.encode())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
