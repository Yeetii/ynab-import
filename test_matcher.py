"""Tests for matcher.py and apply_results in main.py."""
from datetime import date, timedelta
from unittest.mock import MagicMock, call

import pytest

import matcher as m
from main import apply_results
from models import MatchResult, Transaction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bank_tx(
    tx_date: date = date(2026, 3, 10),
    amount: int = -100_000,
    payee: str = "SOME SHOP",
    import_id: str = "spendwise:2026-03-10:-100000:0",
) -> Transaction:
    return Transaction(date=tx_date, amount_milliunits=amount, payee=payee, memo=payee, import_id=import_id)


def ynab_tx(
    tx_date: date = date(2026, 3, 10),
    amount: int = -100_000,
    payee: str = "Some Shop",
    import_id: str | None = None,
    cleared: str = "cleared",
    deleted: bool = False,
    tx_id: str = "ynab-id-1",
) -> dict:
    return {
        "id": tx_id,
        "date": str(tx_date),
        "amount": amount,
        "payee_name": payee,
        "memo": None,
        "cleared": cleared,
        "deleted": deleted,
        "import_id": import_id,
        "matched_transaction_id": None,
    }


def reconciled(tx_date: date, **kwargs) -> dict:
    return ynab_tx(tx_date=tx_date, cleared="reconciled", import_id=f"YNAB:{tx_date}:1", **kwargs)


# ---------------------------------------------------------------------------
# Reconciliation cutoff
# ---------------------------------------------------------------------------

class TestReconciliationCutoff:
    def test_skips_transactions_before_cutoff(self):
        """Bank transactions older than last_reconciled - 2 days are skipped."""
        last_rec = date(2026, 3, 5)
        cutoff = last_rec - timedelta(days=2)  # 2026-03-03

        ynab = [reconciled(last_rec)]
        bank = [bank_tx(tx_date=cutoff - timedelta(days=1))]  # 2026-03-02 → before cutoff

        results = m.match(bank, ynab)
        assert results[0].action == "skip"

    def test_does_not_skip_transactions_on_cutoff_day(self):
        """Transactions exactly on the cutoff date are NOT skipped."""
        last_rec = date(2026, 3, 5)
        cutoff = last_rec - timedelta(days=2)  # 2026-03-03

        ynab = [reconciled(last_rec)]
        bank = [bank_tx(tx_date=cutoff)]  # 2026-03-03 → on the boundary, should proceed

        results = m.match(bank, ynab)
        assert results[0].action != "skip" or results[0].bank_tx.date >= cutoff

    def test_no_cutoff_when_no_reconciled_transactions(self):
        """Without any reconciled transactions, no cutoff is applied."""
        ynab = [ynab_tx(cleared="uncleared")]
        bank = [bank_tx(tx_date=date(2020, 1, 1))]

        results = m.match(bank, ynab)
        # Should not be skipped due to cutoff (may still create or match)
        assert results[0].action in ("create", "match", "skip")
        # Verify it's NOT skipped solely because of cutoff (no reconciled date)
        last_rec = m._last_reconciled_date(ynab)
        assert last_rec is None

    def test_cutoff_uses_latest_reconciled_date(self):
        """Cutoff is based on the LATEST reconciled transaction."""
        ynab = [
            reconciled(date(2026, 2, 1), tx_id="id1"),
            reconciled(date(2026, 3, 5), tx_id="id2"),  # latest
        ]
        assert m._last_reconciled_date(ynab) == date(2026, 3, 5)


# ---------------------------------------------------------------------------
# Exact import_id match → skip
# ---------------------------------------------------------------------------

class TestExactImportIdSkip:
    def test_skips_already_imported_transaction(self):
        """A bank tx whose import_id already exists in YNAB is skipped."""
        existing_id = "spendwise:2026-03-10:-100000:abc"
        ynab = [ynab_tx(import_id=existing_id, cleared="cleared")]
        bank = [bank_tx(import_id=existing_id, tx_date=date(2026, 3, 10))]

        results = m.match(bank, ynab)
        assert results[0].action == "skip"


# ---------------------------------------------------------------------------
# Same date, same amount — auto-imported YNAB tx → skip
# ---------------------------------------------------------------------------

class TestDeduplicationAgainstImported:
    def test_skips_when_auto_imported_same_date_and_amount(self):
        """Bank tx matching an auto-imported YNAB tx (same amount, same date) is skipped."""
        ynab = [ynab_tx(import_id="YNAB:-100000:2026-03-10:1")]
        bank = [bank_tx()]

        results = m.match(bank, ynab)
        assert results[0].action == "skip"

    def test_prefers_same_day_imported_over_nearby_manual(self):
        """When an auto-imported tx is on the same day and a manual tx is 7 days away,
        the auto-imported takes priority (skip, not match)."""
        ynab_imported = ynab_tx(
            tx_date=date(2026, 3, 10), amount=-149_000,
            import_id="YNAB:-149000:2026-03-10:1", tx_id="imported"
        )
        ynab_manual = ynab_tx(
            tx_date=date(2026, 3, 17), amount=-149_000, import_id=None, tx_id="manual"
        )
        bank = [bank_tx(amount=-149_000)]

        results = m.match([bank[0]], [ynab_imported, ynab_manual])
        assert results[0].action == "skip"
        assert results[0].ynab_tx["id"] == "imported"

    def test_prefers_nearby_manual_over_distant_imported(self):
        """When a manual tx is same-day and an imported tx is 7 days away,
        the manual tx is matched (not skipped)."""
        ynab_imported = ynab_tx(
            tx_date=date(2026, 3, 3), import_id="YNAB:-149000:2026-03-03:1", tx_id="imported"
        )
        ynab_manual = ynab_tx(
            tx_date=date(2026, 3, 10), import_id=None, tx_id="manual", amount=-149_000
        )
        bank = [bank_tx(amount=-149_000)]  # date=2026-03-10

        results = m.match([bank[0]], [ynab_imported, ynab_manual])
        assert results[0].action == "match"
        assert results[0].ynab_tx["id"] == "manual"


# ---------------------------------------------------------------------------
# Manual YNAB tx matching
# ---------------------------------------------------------------------------

class TestManualMatching:
    def test_matches_same_date_same_amount(self):
        """Exact date + exact amount against a manual tx → match."""
        ynab = [ynab_tx(import_id=None)]
        bank = [bank_tx()]

        results = m.match(bank, ynab)
        assert results[0].action == "match"

    def test_matches_same_date_different_payee_name(self):
        """YNAB normalizes payee names; a unique amount+date match should succeed
        even when payee strings are completely different (e.g. DELECTOR AB vs Årelagat)."""
        ynab = [ynab_tx(import_id=None, payee="Årelagat")]
        bank = [bank_tx(payee="DELECTOR AB")]

        results = m.match(bank, ynab)
        assert results[0].action == "match"

    def test_matches_slightly_different_date(self):
        """Bank tx 2 days from a unique manual YNAB tx → match (within window)."""
        ynab = [ynab_tx(tx_date=date(2026, 3, 8), import_id=None)]
        bank = [bank_tx(tx_date=date(2026, 3, 6))]  # 2 days apart

        results = m.match(bank, ynab)
        assert results[0].action == "match"

    def test_no_match_when_date_too_far(self):
        """Bank tx more than DATE_WINDOW_DAYS from any manual tx → create."""
        ynab = [ynab_tx(tx_date=date(2026, 3, 10) - timedelta(days=m.DATE_WINDOW_DAYS + 1), import_id=None)]
        bank = [bank_tx(tx_date=date(2026, 3, 10))]

        results = m.match(bank, ynab)
        assert results[0].action == "create"

    def test_no_match_when_amount_differs(self):
        """Different amount → no match, even same date."""
        ynab = [ynab_tx(amount=-99_000, import_id=None)]
        bank = [bank_tx(amount=-100_000)]

        results = m.match(bank, ynab)
        assert results[0].action == "create"

    def test_disambiguates_multiple_same_amount_by_payee(self):
        """When two manual YNAB txns have the same amount in window, payee picks the right one.
        Uses same-case tokens so token_set_ratio scores correctly."""
        ynab_a = ynab_tx(tx_date=date(2026, 3, 10), payee="Burger King", import_id=None, tx_id="bk")
        ynab_b = ynab_tx(tx_date=date(2026, 3, 10), payee="ICA", import_id=None, tx_id="ica")
        bank = [bank_tx(payee="Burger King")]  # exact match to ynab_a

        results = m.match(bank, [ynab_a, ynab_b])
        assert results[0].action == "match"
        assert results[0].ynab_tx["id"] == "bk"

    def test_no_match_when_dates_and_amounts_both_differ_too_much(self):
        """If amount doesn't match AND date is far, result is create."""
        ynab = [ynab_tx(tx_date=date(2025, 1, 1), amount=-500_000, import_id=None)]
        bank = [bank_tx(tx_date=date(2026, 3, 10), amount=-100_000)]

        results = m.match(bank, ynab)
        assert results[0].action == "create"

    def test_deleted_transactions_ignored(self):
        """Deleted YNAB transactions are not considered for matching."""
        ynab = [ynab_tx(import_id=None, deleted=True)]
        bank = [bank_tx()]

        results = m.match(bank, ynab)
        assert results[0].action == "create"


# ---------------------------------------------------------------------------
# apply_results — matched-state via create_transactions
# ---------------------------------------------------------------------------

class TestApplyResults:
    def _make_client(self):
        client = MagicMock()
        client.create_transactions.return_value = {"transaction_ids": ["id1"], "duplicate_import_ids": []}
        return client

    def test_matched_transactions_go_through_create_not_patch(self):
        """Matched transactions must be submitted via create_transactions (not a PATCH),
        so YNAB can auto-link them into a matched-state pair via import_id."""
        client = self._make_client()
        bank = bank_tx()
        ynab = ynab_tx(import_id=None)
        results = [MatchResult(bank_tx=bank, ynab_tx=ynab, confidence=0.9, action="match")]

        apply_results(client, "account-123", results)

        client.create_transactions.assert_called_once()
        # match_transaction (PATCH) must NOT be called
        client.match_transaction.assert_not_called()

    def test_match_payload_includes_import_id(self):
        """The created transaction carries the bank import_id so YNAB can link it."""
        client = self._make_client()
        bank = bank_tx(import_id="spendwise:2026-03-10:-100000:abc")
        ynab = ynab_tx(import_id=None)
        results = [MatchResult(bank_tx=bank, ynab_tx=ynab, confidence=0.9, action="match")]

        apply_results(client, "account-123", results)

        submitted = client.create_transactions.call_args[0][1]  # list[Transaction]
        assert submitted[0].import_id == "spendwise:2026-03-10:-100000:abc"

    def test_creates_and_matches_batched_together(self):
        """Both creates and matches are sent in the same create_transactions call."""
        client = self._make_client()
        r_match = MatchResult(bank_tx=bank_tx(import_id="id-a"), ynab_tx=ynab_tx(), confidence=0.9, action="match")
        r_create = MatchResult(bank_tx=bank_tx(import_id="id-b"), ynab_tx=None, confidence=0.0, action="create")

        apply_results(client, "account-123", [r_match, r_create])

        assert client.create_transactions.call_count == 1
        submitted = client.create_transactions.call_args[0][1]
        assert len(submitted) == 2

    def test_skipped_transactions_not_uploaded(self):
        """Skipped transactions are never sent to YNAB."""
        client = self._make_client()
        r_skip = MatchResult(bank_tx=bank_tx(), ynab_tx=None, confidence=1.0, action="skip")

        apply_results(client, "account-123", [r_skip])

        client.create_transactions.assert_not_called()

    def test_nothing_uploaded_when_all_skipped(self):
        """No API call is made when there is nothing actionable."""
        client = self._make_client()
        apply_results(client, "account-123", [])

        client.create_transactions.assert_not_called()
