from datetime import date, timedelta

from rapidfuzz import fuzz

from models import MatchResult, Transaction

MATCH_THRESHOLD = 0.5
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


def _score_candidates(bank_tx: Transaction, candidates: list[dict]) -> tuple[float, dict, bool] | None:
    """Returns (score, tx, is_unique_match) or None. is_unique_match means only one
    candidate matched by amount+date, so payee scoring was skipped."""
    amount_matches = [tx for tx in candidates if tx["amount"] == bank_tx.amount_milliunits]
    in_window = [(tx, _date_score(bank_tx.date, tx.get("date", ""))) for tx in amount_matches]
    in_window = [(tx, ds) for tx, ds in in_window if ds > 0.0]
    if not in_window:
        return None
    # Unique amount+date match: trust it without payee scoring (YNAB normalizes names)
    if len(in_window) == 1:
        tx, ds = in_window[0]
        return (ds, tx, True)
    # Multiple candidates: use payee to disambiguate
    scored = []
    for tx, ds in in_window:
        ynab_payee = ((tx.get("payee_name") or "") + " " + (tx.get("memo") or "")).strip()
        ps = _payee_score(bank_tx.payee, ynab_payee) if ynab_payee else 1.0
        scored.append((ds * ps, tx))
    best = max(scored, key=lambda x: x[0])
    return (best[0], best[1], False)


def _last_reconciled_date(ynab_txns: list[dict]) -> date | None:
    reconciled = [
        date.fromisoformat(tx["date"][:10])
        for tx in ynab_txns
        if tx.get("cleared") == "reconciled" and not tx.get("deleted", False)
    ]
    return max(reconciled) if reconciled else None


def match(bank_txns: list[Transaction], ynab_txns: list[dict]) -> list[MatchResult]:
    existing_ids = _existing_import_ids(ynab_txns)

    # Skip bank transactions older than last reconciliation date - 2 days
    cutoff = None
    last_rec = _last_reconciled_date(ynab_txns)
    if last_rec:
        cutoff = last_rec - timedelta(days=2)

    active = [tx for tx in ynab_txns if not tx.get("deleted", False)]
    # Manual transactions (no import_id) can be "matched" via the YNAB API
    manual_candidates = [tx for tx in active if not tx.get("import_id")]
    # Already-imported transactions count for deduplication (skip)
    imported_candidates = [tx for tx in active if tx.get("import_id")]

    results: list[MatchResult] = []

    for bank_tx in bank_txns:
        # Older than reconciliation cutoff → skip
        if cutoff and bank_tx.date < cutoff:
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=None, confidence=1.0, action="skip"))
            continue

        # Already imported with this exact import_id → skip
        if bank_tx.import_id in existing_ids:
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=None, confidence=1.0, action="skip"))
            continue

        # Find the closest auto-imported YNAB transaction with the same amount.
        dedup_candidates = [
            tx for tx in imported_candidates
            if tx["amount"] == bank_tx.amount_milliunits
            and _date_score(bank_tx.date, tx.get("date", "")) > 0.0
        ]
        closest_import = max(dedup_candidates, key=lambda tx: _date_score(bank_tx.date, tx.get("date", ""))) if dedup_candidates else None

        # Find the best manual YNAB transaction match.
        best = _score_candidates(bank_tx, manual_candidates)

        # Prefer whichever (imported or manual) is closer by date.
        # If imported is closer (or equal) → skip; if manual is closer → match.
        if closest_import and (not best or _date_score(bank_tx.date, closest_import.get("date", "")) >= best[0]):
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=closest_import, confidence=1.0, action="skip"))
            continue

        if best and (best[2] or best[0] >= MATCH_THRESHOLD):
            results.append(MatchResult(bank_tx=bank_tx, ynab_tx=best[1], confidence=best[0], action="match"))
            continue

        results.append(MatchResult(
            bank_tx=bank_tx,
            ynab_tx=None,
            confidence=best[0] if best else 0.0,
            action="create",
        ))

    return results
