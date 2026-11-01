"""8-K exhibit identification (design doc section 10, verified by spike 1).

`index.json` carries no document types: its `type` field is only an icon name (`text.gif`). The
filing's `{accession}-index.htm` page has a "Document Format Files" table with a real Type column
(`8-K`, `EX-99.1`, `EX-5.1`, `GRAPHIC`, ...). The rule: fetch every document whose type is `EX-99`
or `EX-99.N`, which are the press releases and investor material that carry an 8-K's substance.
Other types (legal opinions, graphics, XBRL files) are ignored. An 8-K with no EX-99 is valid; it
is committed with its primary document only.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from html.parser import HTMLParser

from .edgar_client import EdgarError

_EX99 = re.compile(r"^EX-99(\.\d+)?$")
_TABLE_SUMMARY = "Document Format Files"


class ExhibitParseError(EdgarError):
    """The index page did not have the expected table; refuse rather than commit a silent gap."""


@dataclass(frozen=True)
class ExhibitRef:
    label: str  # e.g. "ex99-1"; becomes the filename prefix under exhibits/
    filename: str


# Takes the bytes of the filing's `-index.htm` page.
ExhibitSelector = Callable[[bytes], list[ExhibitRef]]


class _IndexPage(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.found_table = False
        self.rows: list[list[str]] = []
        self._in_table = False
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._in_table = dict(attrs).get("summary") == _TABLE_SUMMARY
            self.found_table = self.found_table or self._in_table
        elif self._in_table and tag == "tr":
            self._row = []
        elif self._in_table and tag == "td":
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).replace("\xa0", " ").split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def select_ex99(index_htm: bytes) -> list[ExhibitRef]:
    page = _IndexPage()
    page.feed(index_htm.decode("utf-8", errors="replace"))
    if not page.found_table:
        raise ExhibitParseError(f"no '{_TABLE_SUMMARY}' table in the filing index page")
    refs = []
    for row in page.rows:
        # Columns: Seq, Description, Document, Type, Size. Header rows use <th>, so are skipped.
        if len(row) != 5 or not _EX99.match(row[3]):
            continue
        filename = row[2].split(" ")[0]  # the cell may end with an "iXBRL" badge
        label = "ex" + row[3].removeprefix("EX-").replace(".", "-")
        refs.append(ExhibitRef(label, filename))
    return refs
