from pathlib import Path

import pytest

from sec_ingest.exhibits import ExhibitParseError, ExhibitRef, select_ex99

FIXTURES = Path(__file__).parent / "fixtures" / "spike1"


def page(ticker: str, acc: str) -> bytes:
    return (FIXTURES / f"{ticker}_8-K_{acc}_{acc}-index.htm").read_bytes()


@pytest.mark.parametrize(
    ("ticker", "acc", "expected"),
    [
        ("AAPL", "0000320193-26-000018", [ExhibitRef("ex99-1", "a8-kex991q3202606272026.htm")]),
        (
            "XOM",
            "0002115436-26-000018",
            [
                ExhibitRef("ex99-1", "livef8k3q26991.htm"),
                ExhibitRef("ex99-2", "livef8k3q26992.htm"),
            ],
        ),
        ("JPM", "0001628280-26-065616", [ExhibitRef("ex99", "a3q26_earningsxpresentat.htm")]),
        ("JPM", "0001193125-26-426704", []),  # only an EX-5.1 legal opinion
    ],
)
def test_real_index_pages(ticker: str, acc: str, expected: list[ExhibitRef]) -> None:
    assert select_ex99(page(ticker, acc)) == expected


def test_page_without_the_table_is_an_error() -> None:
    with pytest.raises(ExhibitParseError):
        select_ex99(b"<html><body>Request blocked</body></html>")


def test_ignores_ex99_lookalikes_and_xbrl_types() -> None:
    html = b"""<table summary="Document Format Files">
      <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
      <tr><td>1</td><td>x</td><td><a>a.htm</a> <span>iXBRL</span></td><td>8-K</td><td>1</td></tr>
      <tr><td>2</td><td>x</td><td><a>b.htm</a></td><td>EX-99.1</td><td>1</td></tr>
      <tr><td>3</td><td>x</td><td><a>c.htm</a></td><td>EX-991</td><td>1</td></tr>
      <tr><td>4</td><td>x</td><td><a>d.htm</a></td><td>EX-101.SCH</td><td>1</td></tr>
      <tr><td>5</td><td>x</td><td><a>e.htm</a></td><td>EX-99.10</td><td>1</td></tr></table>"""
    assert select_ex99(html) == [ExhibitRef("ex99-1", "b.htm"), ExhibitRef("ex99-10", "e.htm")]
