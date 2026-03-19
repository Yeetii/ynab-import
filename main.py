#!/usr/bin/env python3
"""YNAB Bank Import Tool

Usage:
    python main.py --bank handelsbanken --file march.xlsx [--dry-run] [--since 2026-01-01] [--auto-confirm]
    python main.py --bank spendwise --file march.xlsx [--dry-run]
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table

import config
import matcher as match_module
from adapters.handelsbanken import HandelsbankenAdapter, parse_balance
from adapters.spendwise import SpendwiseAdapter
from models import MatchResult
from ynab_client import YnabClient

console = Console()

ADAPTERS = {
    "handelsbanken": HandelsbankenAdapter,
    "spendwise": SpendwiseAdapter,
}

ACTION_STYLES = {
    "match": ("[green]✓ MATCH[/green]", "green"),
    "create": ("[blue]+ CREATE[/blue]", "blue"),
    "skip": ("[dim]~ SKIP[/dim]", "dim"),
}


def format_amount(milliunits: int) -> str:
    amount = milliunits / 1000
    return f"{amount:+,.2f} SEK"


def print_summary(results: list[MatchResult]) -> None:
    table = Table(show_header=True, header_style="bold")
    table.add_column("Action", width=14)
    table.add_column("Date", width=12)
    table.add_column("Amount", width=14, justify="right")
    table.add_column("Bank Payee")
    table.add_column("YNAB Match")
    table.add_column("Conf", width=6, justify="right")

    for r in results:
        action_label, style = ACTION_STYLES[r.action]
        ynab_payee = ""
        conf_str = ""
        if r.action == "match" and r.ynab_tx:
            ynab_payee = r.ynab_tx.get("payee_name") or ""
            conf_str = f"{r.confidence:.2f}"
        elif r.action == "skip":
            ynab_payee = "(already imported)"
        elif r.action == "create" and r.confidence > 0:
            conf_str = f"{r.confidence:.2f}"

        table.add_row(
            action_label,
            str(r.bank_tx.date),
            format_amount(r.bank_tx.amount_milliunits),
            r.bank_tx.payee[:40],
            ynab_payee[:40],
            conf_str,
            style=style if r.action == "skip" else None,
        )

    console.print(table)

    matches = sum(1 for r in results if r.action == "match")
    creates = sum(1 for r in results if r.action == "create")
    skips = sum(1 for r in results if r.action == "skip")
    console.print(
        f"\n[green]{matches} match(es)[/green], "
        f"[blue]{creates} to create[/blue], "
        f"[dim]{skips} skipped[/dim]"
    )


def apply_results(client: YnabClient, account_id: str, results: list[MatchResult]) -> None:
    matches = [r for r in results if r.action == "match"]
    creates = [r for r in results if r.action == "create"]

    # Matches and creates both go through create_transactions.
    # YNAB auto-links imported transactions (via import_id) to existing manual
    # transactions with the same amount/date, creating the matched_transaction_id
    # chain exactly like YNAB's own bank sync does. This also corrects amounts.
    all_to_import = matches + creates
    if all_to_import:
        label = []
        if matches:
            label.append(f"matching {len(matches)}")
        if creates:
            label.append(f"creating {len(creates)}")
        console.print(f"\nImporting ({', '.join(label)}) transaction(s)...")
        chunk_size = 1000
        for i in range(0, len(all_to_import), chunk_size):
            chunk = all_to_import[i : i + chunk_size]
            txns = [r.bank_tx for r in chunk]
            try:
                result = client.create_transactions(account_id, txns)
                dups = result.get("duplicate_import_ids", [])
                created_count = len(result.get("transaction_ids", []))
                console.print(f"  [green]✓[/green] Imported {created_count} transaction(s)" + (f", {len(dups)} duplicate(s) skipped" if dups else ""))
            except Exception as e:
                console.print(f"  [red]✗[/red] Failed to import batch: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import bank transactions into YNAB")
    parser.add_argument("--bank", required=True, choices=list(ADAPTERS), help="Bank adapter to use")
    parser.add_argument("--file", required=True, type=Path, help="Path to export file (XLSX or CSV)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and match but do not write to YNAB")
    parser.add_argument("--since", type=date.fromisoformat, help="Only import transactions on/after this date (YYYY-MM-DD)")
    parser.add_argument("--auto-confirm", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    filepath = args.file.expanduser().resolve()
    if not filepath.exists():
        console.print(f"[red]File not found:[/red] {filepath}")
        sys.exit(1)

    account_id = config.ACCOUNT_IDS.get(args.bank)
    if not account_id:
        console.print(f"[red]No YNAB account ID configured for '{args.bank}'. Set YNAB_ACCOUNT_{args.bank.upper()} in .env[/red]")
        sys.exit(1)

    # Parse bank file
    console.print(f"Parsing [bold]{filepath.name}[/bold] with {args.bank} adapter...")
    adapter = ADAPTERS[args.bank]()
    bank_txns = adapter.parse(filepath)

    if args.since:
        bank_txns = [t for t in bank_txns if t.date >= args.since]

    if not bank_txns:
        console.print("[yellow]No transactions found in file.[/yellow]")
        sys.exit(0)

    console.print(f"Found [bold]{len(bank_txns)}[/bold] transaction(s) in file.")

    # Fetch YNAB transactions
    earliest = min(t.date for t in bank_txns) - timedelta(days=10)
    console.print(f"Fetching YNAB transactions since {earliest}...")
    client = YnabClient()
    ynab_txns = client.get_transactions(account_id, earliest)
    console.print(f"Fetched [bold]{len(ynab_txns)}[/bold] YNAB transaction(s).")

    # Match
    results = match_module.match(bank_txns, ynab_txns)

    # Display
    console.print()
    print_summary(results)

    actionable = [r for r in results if r.action in ("match", "create")]
    if not actionable:
        console.print("\n[dim]Nothing to do.[/dim]")
        return

    if args.dry_run:
        console.print("\n[yellow]Dry run — no changes written.[/yellow]")
        return

    # Confirm
    if not args.auto_confirm:
        matches_n = sum(1 for r in results if r.action == "match")
        creates_n = sum(1 for r in results if r.action == "create")
        parts = []
        if matches_n:
            parts.append(f"{matches_n} match(es)")
        if creates_n:
            parts.append(f"create {creates_n} new")
        prompt = f"\nApply {', '.join(parts)}? [y/N] "
        answer = console.input(prompt).strip().lower()
        if answer != "y":
            console.print("[dim]Aborted.[/dim]")
            return

    apply_results(client, account_id, results)
    console.print("\n[green]Done.[/green]")

    # Reconciliation (Handelsbanken only): if file balance matches YNAB cleared balance
    # within 150 SEK, mark all cleared transactions as reconciled.
    if args.bank == "handelsbanken":
        file_balance = parse_balance(filepath)
        if file_balance is not None:
            account = client.get_account(account_id)
            ynab_cleared = account["cleared_balance"]  # milliunits
            diff_sek = abs(file_balance - ynab_cleared) / 1000
            console.print(f"\nBalance check: file={file_balance/1000:.2f} SEK, YNAB cleared={ynab_cleared/1000:.2f} SEK, diff={diff_sek:.2f} SEK")
            if diff_sek <= 150:
                if not args.auto_confirm:
                    answer = console.input(f"Reconcile account (diff {diff_sek:.2f} SEK ≤ 150)? [y/N] ").strip().lower()
                    if answer != "y":
                        console.print("[dim]Reconciliation skipped.[/dim]")
                        return
                count = client.reconcile(account_id)
                console.print(f"[green]✓[/green] Reconciled {count} transaction(s).")
            else:
                console.print(f"[yellow]Skipping reconciliation — difference {diff_sek:.2f} SEK exceeds 150 SEK.[/yellow]")


if __name__ == "__main__":
    main()
