"""Tests for SpendwiseAdapter.parse_balance.

Mock data mirrors the structure of a real Spendwise XLSX export:
  Row 0: export header
  Row 1: empty
  Row 2: section label
  Row 3: column headers  (Datum | Bokfört | Specifikation | Ort | Valuta | Utl. belopp | Belopp)
  Row 4+: transaction rows (or currency-rate notes with no date)

The balance is the sum of all charges where the *booking date* (Bokfört) falls
within the previous and current calendar month relative to the latest booking
date in the file, excluding monthly payment rows ("BETALT BG …").
"""
import io
from datetime import date
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pandas as pd
import pytest

from adapters.spendwise import parse_balance

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COLUMNS = ["Datum", "Bokfört", "Specifikation", "Ort", "Valuta", "Utl. belopp", "Belopp"]


def _make_xlsx(rows: list[list]) -> Path:
    """Write *rows* (each a list matching _COLUMNS) to an in-memory XLSX and
    return a Path-like object backed by a BytesIO buffer via tmp_path fixture
    (callers must use the helper fixture below instead).
    """
    raise NotImplementedError("use make_xlsx fixture")


def _build_workbook(rows: list[list]) -> bytes:
    """Build a minimal Spendwise XLSX in-memory and return raw bytes."""
    wb = openpyxl.Workbook()
    ws = wb.active

    # Rows 1-3: header area
    ws.append(["Transaktionsexport", None, None, None, None, None, "2026-05-06"])
    ws.append([None] * 7)
    ws.append(["Totalt övriga händelser"] + [None] * 6)
    # Row 4: column headers
    ws.append(_COLUMNS)
    # Data rows
    for row in rows:
        ws.append(row)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


@pytest.fixture()
def make_xlsx(tmp_path):
    """Return a factory: make_xlsx(rows) → Path to a temp XLSX file."""
    def factory(rows: list[list]) -> Path:
        data = _build_workbook(rows)
        p = tmp_path / "test.xlsx"
        p.write_bytes(data)
        return p
    return factory


def _tx(datum: str, bokfort: str, spec: str, amount: str, currency: str = "SEK") -> list:
    return [datum, bokfort, spec, "STOCKHOLM", currency, "0", amount]


# ---------------------------------------------------------------------------
# Basic balance calculation
# ---------------------------------------------------------------------------

class TestParseBalance:
    def test_returns_none_for_empty_file(self, make_xlsx):
        p = make_xlsx([])
        assert parse_balance(p) is None

    def test_single_charge_in_current_month(self, make_xlsx):
        rows = [_tx("2026-05-01", "2026-05-02", "SOME SHOP", "1000")]
        p = make_xlsx(rows)
        assert parse_balance(p) == -1_000_000

    def test_single_charge_in_previous_month(self, make_xlsx):
        rows = [
            _tx("2026-04-15", "2026-04-16", "SOME SHOP", "500"),
            _tx("2026-05-01", "2026-05-02", "OTHER SHOP", "200"),
        ]
        p = make_xlsx(rows)
        assert parse_balance(p) == -700_000

    def test_excludes_betalt_payment_rows(self, make_xlsx):
        rows = [
            _tx("2026-04-30", "2026-04-30", "BETALT BG DATUM 260430", "-16762.39"),
            _tx("2026-04-15", "2026-04-16", "GROCERY STORE A", "531.58"),
            _tx("2026-05-03", "2026-05-04", "GROCERY STORE B", "178.8"),
        ]
        p = make_xlsx(rows)
        expected = -round((531.58 + 178.8) * 1000)
        assert parse_balance(p) == expected

    def test_excludes_transactions_older_than_2_months(self, make_xlsx):
        rows = [
            # March (2 months before May → included as start of window)
            _tx("2026-03-10", "2026-03-11", "OLD SHOP", "999"),
            # April
            _tx("2026-04-10", "2026-04-11", "APRIL SHOP", "100"),
            # May
            _tx("2026-05-01", "2026-05-02", "MAY SHOP", "50"),
        ]
        p = make_xlsx(rows)
        # Window = April 1 to May 2 (max booking date); March is excluded
        assert parse_balance(p) == -150_000

    def test_march31_transaction_booked_april1_is_included(self, make_xlsx):
        """Transaction dated March 31 but booked April 1 counts in April cycle."""
        rows = [
            _tx("2026-03-31", "2026-04-01", "SUBSCRIPTION A", "9"),
            _tx("2026-04-15", "2026-04-16", "GROCERY STORE A", "100"),
            _tx("2026-05-01", "2026-05-02", "GROCERY STORE B", "50"),
        ]
        p = make_xlsx(rows)
        assert parse_balance(p) == -159_000

    def test_march31_transaction_booked_march31_is_excluded(self, make_xlsx):
        """Transaction dated and booked March 31 is outside the April-May window."""
        rows = [
            _tx("2026-03-31", "2026-03-31", "RETAILER A", "149"),
            _tx("2026-04-15", "2026-04-16", "GROCERY STORE A", "100"),
            _tx("2026-05-01", "2026-05-02", "GROCERY STORE B", "50"),
        ]
        p = make_xlsx(rows)
        assert parse_balance(p) == -150_000

    def test_currency_rate_rows_are_ignored(self, make_xlsx):
        """Rows without a valid booking date (currency-rate notes) are skipped."""
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Transaktionsexport"] + [None] * 6)
        ws.append([None] * 7)
        ws.append(["Totalt övriga händelser"] + [None] * 6)
        ws.append(_COLUMNS)
        ws.append(_tx("2026-04-10", "2026-04-11", "SUBSCRIPTION B", "21.59", "USD"))
        # Currency rate note row
        ws.append(["Valutakurs: 9.725225 Valutapåslag ingår med 2,00 %"] + [None] * 6)
        ws.append(_tx("2026-05-01", "2026-05-02", "GROCERY STORE A", "100"))
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            f.write(buf.read())
            path = Path(f.name)
        try:
            result = parse_balance(path)
            assert result == -round((21.59 + 100) * 1000)
        finally:
            os.unlink(path)

    def test_reference_date_overrides_max_date(self, make_xlsx):
        """reference_date parameter sets the calendar window explicitly."""
        rows = [
            _tx("2026-03-10", "2026-03-11", "MARCH SHOP", "200"),
            _tx("2026-04-10", "2026-04-11", "APRIL SHOP", "100"),
            _tx("2026-05-01", "2026-05-02", "MAY SHOP", "50"),
        ]
        p = make_xlsx(rows)
        # With reference_date in April, window = March 1–April 11; May excluded
        result = parse_balance(p, reference_date=date(2026, 4, 30))
        assert result == -300_000  # 200 + 100

    def test_swedish_decimal_amounts(self, make_xlsx):
        """Amounts written with comma as decimal separator are parsed correctly."""
        rows = [_tx("2026-04-15", "2026-04-16", "ICA", "1 234,56")]
        p = make_xlsx(rows)
        assert parse_balance(p) == -1_234_560

    def test_real_world_balance(self, make_xlsx):
        """Reproduces the known balance of 13 399,49 SEK from the real file.

        All 57 charge rows from the real export are included.
        Key edge-cases:
          - APPLE.COM/BILL dated 2026-03-31 but booked 2026-04-01 → INCLUDED
          - DELECTOR AB dated and booked 2026-03-31 → EXCLUDED
          - BETALT BG payment on 2026-04-30 → EXCLUDED
          - ARE SKIDSPORT A dated 2026-04-30 but booked 2026-05-04 → INCLUDED
        """
        rows = [
            # March 31, booked April 1 → INCLUDED
            _tx("2026-03-31", "2026-04-01", "SUBSCRIPTION A", "9"),
            # March 31, booked March 31 → EXCLUDED (prior billing cycle)
            _tx("2026-03-31", "2026-03-31", "RETAILER A", "149"),
            # April payment → EXCLUDED
            _tx("2026-04-30", "2026-04-30", "BETALT BG DATUM 260430", "-16762.39"),
            # April charges
            _tx("2026-04-01", "2026-04-02", "CLOUD SERVICE A", "2.53"),
            _tx("2026-04-04", "2026-04-07", "TRANSIT MERCHANT A", "122"),
            _tx("2026-04-04", "2026-04-07", "TRANSIT MERCHANT B", "1476"),
            _tx("2026-04-05", "2026-04-07", "SUBSCRIPTION B", "21.59"),
            _tx("2026-04-05", "2026-04-07", "TRANSIT MERCHANT A", "28"),
            _tx("2026-04-05", "2026-04-07", "TRANSIT MERCHANT A", "69"),
            _tx("2026-04-05", "2026-04-07", "KIOSK A", "26"),
            _tx("2026-04-07", "2026-04-07", "ONLINE RETAILER A", "671"),
            _tx("2026-04-08", "2026-04-09", "DONATION A", "50"),
            _tx("2026-04-09", "2026-04-10", "SUBSCRIPTION B", "90.42"),
            _tx("2026-04-09", "2026-04-10", "SUBSCRIPTION C", "178.6"),
            _tx("2026-04-09", "2026-04-09", "SUBSCRIPTION B", "262.35"),
            _tx("2026-04-10", "2026-04-13", "RESTAURANT A", "281.98"),
            _tx("2026-04-10", "2026-04-13", "TRAVEL MERCHANT A", "68.87"),
            _tx("2026-04-10", "2026-04-13", "TRAVEL MERCHANT B", "22.5"),
            _tx("2026-04-10", "2026-04-13", "TRAVEL RETAILER A", "39.54"),
            _tx("2026-04-11", "2026-04-13", "PARKING A", "45.94"),
            _tx("2026-04-12", "2026-04-13", "RESTAURANT B", "73.04"),
            _tx("2026-04-12", "2026-04-13", "TRANSIT MERCHANT C", "340"),
            _tx("2026-04-12", "2026-04-13", "PARKING B", "45.94"),
            _tx("2026-04-12", "2026-04-13", "FUEL STATION A", "632.89"),
            _tx("2026-04-12", "2026-04-13", "GROCERY STORE A", "73.13"),
            _tx("2026-04-12", "2026-04-13", "RENTAL SERVICE A", "352.59"),
            _tx("2026-04-12", "2026-04-13", "PARKING C", "725.82"),
            _tx("2026-04-12", "2026-04-13", "SERVICE PROVIDER A", "140.11"),
            _tx("2026-04-13", "2026-04-14", "CONVENIENCE STORE A", "96"),
            _tx("2026-04-13", "2026-04-14", "RESTAURANT C", "170"),
            _tx("2026-04-13", "2026-04-14", "ENTERTAINMENT A", "49"),
            _tx("2026-04-14", "2026-04-15", "COFFEE SHOP A", "119"),
            _tx("2026-04-14", "2026-04-15", "RETAIL SHOP A", "124"),
            _tx("2026-04-14", "2026-04-15", "RETAIL SHOP A", "14.5"),
            _tx("2026-04-15", "2026-04-16", "ONLINE MARKETPLACE A", "366"),
            _tx("2026-04-16", "2026-04-17", "SUBSCRIPTION A", "30"),
            _tx("2026-04-16", "2026-04-17", "TRANSIT MERCHANT D", "145"),
            _tx("2026-04-16", "2026-04-17", "GROCERY STORE B", "304.11"),
            _tx("2026-04-18", "2026-04-20", "ELECTRONICS A", "110"),
            _tx("2026-04-20", "2026-04-22", "GROCERY STORE B", "531.58"),
            _tx("2026-04-20", "2026-04-22", "PARKING D", "5"),
            _tx("2026-04-21", "2026-04-22", "BAKERY A", "404"),
            _tx("2026-04-21", "2026-04-22", "GROCERY STORE B", "120.04"),
            _tx("2026-04-23", "2026-04-24", "SUBSCRIPTION D", "239.45"),
            _tx("2026-04-23", "2026-04-23", "SUBSCRIPTION A", "12"),
            _tx("2026-04-25", "2026-04-27", "TICKET SERVICE A", "395"),
            _tx("2026-04-25", "2026-04-27", "GROCERY STORE C", "209.43"),
            _tx("2026-04-26", "2026-04-27", "GROCERY STORE D", "369.29"),
            _tx("2026-04-27", "2026-04-29", "GROCERY STORE D", "360.44"),
            _tx("2026-04-28", "2026-04-29", "RETAILER A", "149"),
            # Dated April 30 but booked May 4 → INCLUDED (in May window)
            _tx("2026-04-30", "2026-05-04", "SPORTS RETAILER A", "1704"),
            # May charges
            _tx("2026-05-01", "2026-05-04", "CLOUD SERVICE B", "2.44"),
            _tx("2026-05-01", "2026-05-04", "ACCOMMODATION A", "124"),
            _tx("2026-05-01", "2026-05-04", "ACCOMMODATION A", "69"),
            _tx("2026-05-01", "2026-05-04", "GROCERY STORE C", "201.73"),
            _tx("2026-05-02", "2026-05-04", "CAR WASH A", "299"),
            _tx("2026-05-03", "2026-05-04", "ONLINE SERVICE A", "501.9"),
            _tx("2026-05-03", "2026-05-04", "GROCERY STORE D", "178.8"),
            _tx("2026-05-05", "2026-05-06", "SUBSCRIPTION E", "146.94"),
        ]
        p = make_xlsx(rows)
        assert parse_balance(p) == -13_399_490
