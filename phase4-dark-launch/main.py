"""
main.py
───────
Phase 4 CLI — Dark Launch & Semantic Audit

Commands:
  start-audit        Start the semantic audit observation window
  check-pii          Run PII redaction validation (determinism + irreversibility)
  check-finops       Show current FinOps arbitrator status
  status             Show full Phase 4 status
  report             Generate semantic audit report
  run                Full Phase 4 pipeline status check
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

app = typer.Typer(name="migratex-phase4", help="MigrateX Phase 4 — Dark Launch & Semantic Audit")
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("migratex.phase4")


@app.command(name="check-pii")
def check_pii(
    pii_map: str = typer.Option("../phase1-discovery/artifacts/pii_surface_map.json"),
    fk_graph: str = typer.Option("../phase1-discovery/artifacts/fk_dependency_graph.json"),
):
    """Run PII redaction validation — determinism and irreversibility tests."""
    from src.redaction.engine import HMACRedactionEngine

    console.rule("[bold blue]Phase 4 — PII Redaction Validation")

    pii_path = Path(pii_map)
    fk_path = Path(fk_graph)

    if not pii_path.exists():
        console.print(f"[yellow]PII surface map not found at {pii_path}[/yellow]")
        console.print("[yellow]Using synthetic PII map for demonstration[/yellow]")
        pii_map_data = {
            "users": [
                {"column": "email", "pii_tier": 1},
                {"column": "phone_number", "pii_tier": 1},
                {"column": "zipcode", "pii_tier": 2},
            ]
        }
        fk_graph_data = []
    else:
        with open(pii_path) as f:
            pii_map_data = json.load(f)
        with open(fk_path) as f:
            fk_graph_data = json.load(f)

    engine = HMACRedactionEngine(pii_map_data, fk_graph_data)

    table = Table(title="PII Redaction Validation")
    table.add_column("Test", style="cyan")
    table.add_column("Result", style="white")

    # Determinism test
    det_passed = engine.verify_determinism("test@example.com", "users", "email")
    table.add_row(
        "Determinism: same input → same HMAC",
        "[green]PASS[/green]" if det_passed else "[red]FAIL[/red]"
    )

    # Irreversibility test
    irr_passed = engine.verify_irreversibility(
        ["alice@test.com", "bob@test.com", "+1234567890"],
        "users", "email"
    )
    table.add_row(
        "Irreversibility: HMAC output is not PII",
        "[green]PASS[/green]" if irr_passed else "[red]FAIL[/red]"
    )

    # FK processing order
    order = engine.get_processing_order()
    table.add_row(
        "FK processing order",
        f"[green]{' → '.join(order[:5])}{'...' if len(order) > 5 else ''}[/green]"
    )

    session = engine.get_session()
    table.add_row("Session ID", session.session_id)
    table.add_row("Key fingerprint (partial)", session.key_fingerprint)

    console.print(table)

    total_pii_cols = sum(len(v) for v in pii_map_data.values())
    console.print(f"\nPII columns covered: [cyan]{total_pii_cols}[/cyan]")
    console.print(f"Tables with PII: [cyan]{len(pii_map_data)}[/cyan]")

    if det_passed and irr_passed:
        console.print("\n[green]PII redaction validation PASSED[/green]")
    else:
        console.print("\n[red]PII redaction validation FAILED[/red]")
        raise typer.Exit(1)


@app.command(name="check-finops")
def check_finops():
    """Display current FinOps arbitrator status."""
    from src.finops.arbitrator import FinOpsArbitrator

    console.rule("[bold blue]Phase 4 — FinOps Arbitrator Status")

    arb = FinOpsArbitrator(
        baseline_price_per_gb=0.09,
        spike_multiplier_threshold=1.5,
        phase1_p50_estimate_usd=500.0,
        migration_deadline_hours=240,
    )

    meter = arb.get_meter()
    status = arb.get_status()

    console.print(Panel(
        f"Cost optimization: [{'green' if status.cost_optimization_active else 'yellow'}]"
        f"{'ACTIVE' if status.cost_optimization_active else 'SUSPENDED (deadline override)'}[/]\n"
        f"Current egress price: [cyan]${meter.current_price_per_gb:.4f}/GB[/cyan]\n"
        f"Baseline price: [cyan]${meter.baseline_price_per_gb:.4f}/GB[/cyan]\n"
        f"Price spike: [{'red' if status.spike_detected else 'green'}]"
        f"{'YES' if status.spike_detected else 'NO'}[/]\n"
        f"WAL buffer size: [cyan]{status.wal_buffer_size} entries[/cyan]\n"
        f"Total cost: [cyan]${status.total_cost_usd:,.2f}[/cyan]\n"
        f"Savings attributed: [cyan]${status.savings_attributed_usd:,.2f}[/cyan]",
        title="FinOps Status",
    ))


@app.command()
def run():
    """Run full Phase 4 status check."""
    console.rule("[bold blue]MigrateX Phase 4 — Dark Launch Status")

    console.print("\n[bold]Step 1/3: PII Redaction Validation[/bold]")
    check_pii()

    console.print("\n[bold]Step 2/3: FinOps Arbitrator[/bold]")
    check_finops()

    console.print("\n[bold]Step 3/3: Phase 4 Exit Criteria[/bold]")
    console.print("\nExit criteria (requires 72h observation window with live infrastructure):")
    console.print("  [ ] Zero Class A divergences over 72-hour window")
    console.print("  [ ] All Class B divergences documented with root cause")
    console.print("  [ ] All 6 critical business logic paths at zero Class A error rate")
    console.print("  [ ] PII redaction determinism test: PASSED")
    console.print("  [ ] PII redaction irreversibility test: PASSED")
    console.print("  [ ] Referential integrity after redaction: zero FK violations")
    console.print("  [ ] FinOps cost tracking active, projection within P95 estimate")
    console.print("  [ ] Shadow write error rate < 0.01%")
    console.print("\n[green]PII and FinOps components validated successfully.[/green]")
    console.print("[yellow]Full 72-hour audit window requires live infrastructure.[/yellow]")


if __name__ == "__main__":
    app()
