from datetime import date, timedelta

from rapidfuzz import fuzz

from models import MatchResult, Transaction

MATCH_THRESHOLD = 0.6
DATE_WINDOW_DAYS = 10


def _date_score(bank_date: date, ynab_date_str: str) -> float:
    try:
        ynab_date = date.fromisoformat(ynab_date_str[:10])
    except ValueError:
        return 0.0
    diff = abs((bank_date - ynab_date).days)
    if diff > DATE_WINDOW_DAYS:
        return 0.0
    return 1.0 / (1.0 + diff)


def _payee_score(bank_payee: str, ynab_payee: str | None) -> float:
    if not ynab_payee:
        return 0.0
    return fuzz.token_set_ratio(bank_payee, ynab_payee) / 100.0


def _existing_import_ids(ynab_txns: list[dict]) -> set[str]:
    return {tx["import_id"] for tx in ynab_txns if tx.get("import_id")}


def _score_candidates(bank_tx: Transaction, candidates: list[dict]) -> tuple[float, dict] | None:
    amount_matches = [tx for tx in candidates if tx["amount"] == bank_tx.amount_milliunits]
    scored = []
    for tx in amount_matches:
        ds = _date_score(bank_tx.date, tx.get("date", ""))
        if ds == 0.0:
            continue
        ynab_payee = (tx.get("payee_name") or "") + " " + (tx.get("memo") or "")
        ps = _payee_score(bank_tx.payee, ynab_payee.strip())
        scored.append((ds * ps, tx))
    return max(scored, key=lambda x: x[0]) if scored else None


def match(bank_txns: list[Transaction], ynab_txns: list[dict]) -> list[MatchResult]:
    existing_ids = _existing_import_ids(ynab_txns)

    active = [tx for tx in ynab_txns if not tx.get("deleted", False)]
    # Manual transactions (no import_id) can be "matched" via the YNAB API
    manual_candidates = [tx for tx in active if not tx.get("import_id")]
    # Already-imported transactions count for deduplication (skip)
    imported_candidates = [tx for tx in active if tx.get("import_id")]

    results: list[MatchResult] = []

    for bank_tx in bank_txns:
        # Already imported with this exact import_id → skip
        if bank_tx.import_id in existing_ids:
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=None, confidence=1.0, action="skip"))
            continue

        # Try to match against manual transactions
        best = _score_candidates(bank_tx, manual_candidates)
        if best and best[0] >= MATCH_THRESHOLD:
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=best[1], confidence=best[0], action="match"))
            continue

        # Check if already present as an auto-imported transaction → skip (avoid duplicate)
        # Use date-proximity only (no payee scoring) since YNAB normalizes payee names
        dedup = [
            tx for tx in imported_candidates
            if tx["amount"] == bank_tx.amount_milliunits
            and _date_score(bank_tx.date, tx.get("date", "")) > 0.0
        ]
        if dedup:
            closest = max(dedup, key=lambda tx: _date_score(bank_tx.date, tx.get("date", "")))
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=closest, confidence=1.0, action="skip"))
            continue

        results.append(MatchResult(
            bank_tx=bank_tx,
            ynab_tx=None,
            confidence=best[0] if best else 0.0,
            action="create",
        ))

    return results
