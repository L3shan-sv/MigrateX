"""
main.py
───────
Phase 5 CLI — Event Horizon & Atomic Cutover

Commands:
  preflight       Run pre-flight checklist
  signoff         Sign off a manual pre-flight item
  final-call      Issue Final Call notification (T-30 min)
  execute         Execute the atomic cutover sequence (requires JWT token)
  abort           Abort the migration
  postpone        Postpone cutover with reason code
  monitor         Start PONR Event Horizon monitor
  status          Show current Phase 5 status
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

app = typer.Typer(name="migratex-phase5", help="MigrateX Phase 5 — Event Horizon & Cutover")
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("migratex.phase5")


def _make_checklist():
    from src.preflight.checklist import PreflightChecklist
    return PreflightChecklist(
        lag_max_seconds=5.0,
        ponr_sla_usd=5_000.0,
        anomaly_threshold=0.7,
        merkle_max_age_minutes=30,
    )


@app.command()
def preflight():
    """Run the pre-flight checklist."""
    console.rule("[bold blue]Phase 5 — Pre-flight Checklist")
    checklist = _make_checklist()
    result = checklist.run()

    table = Table(title="Pre-flight Checks")
    table.add_column("ID", style="cyan")
    table.add_column("Check", style="white")
    table.add_column("Status", style="white")
    table.add_column("Actual", style="white")
    table.add_column("Threshold", style="white")

    for item in result.items:
        color = {
            "green": "green", "red": "red", "manual": "yellow", "pending": "dim"
        }.get(item.status.value, "white")
        table.add_row(
            item.check_id,
            item.name,
            f"[{color}]{item.status.value.upper()}[/{color}]",
            item.actual_value,
            item.threshold,
        )

    console.print(table)

    if result.all_green:
        console.print("\n[green]ALL GREEN — cutover window can proceed[/green]")
    else:
        if result.blocking_items:
            console.print(f"\n[red]BLOCKING: {result.blocking_items}[/red]")
        if result.manual_items_pending:
            console.print(f"\n[yellow]MANUAL PENDING: {result.manual_items_pending}[/yellow]")
            console.print("[yellow]Run 'python main.py signoff <check_id>' for each[/yellow]")


@app.command()
def signoff(check_id: str, operator: str = typer.Option("operator", help="Operator email")):
    """Sign off a manual pre-flight item."""
    checklist = _make_checklist()
    success = checklist.sign_off_manual(check_id, operator)
    if success:
        console.print(f"[green]Signed off {check_id} as {operator}[/green]")
    else:
        console.print(f"[red]Invalid check ID: {check_id}[/red]")


@app.command(name="final-call")
def final_call():
    """Issue Final Call notification (T-30 minutes before cutover)."""
    from src.ponr.monitor import LivePONRMonitor, LiveMigrationState

    console.rule("[bold red]Phase 5 — FINAL CALL (T-30 minutes)")

    monitor = LivePONRMonitor(n_simulations=1000, sla_threshold_usd=5_000.0, seed=42)
    state = LiveMigrationState(
        total_data_bytes=int(100e9),
        bytes_transferred=int(95e9),
        write_rate_bytes_per_sec=50e6,
        replication_lag_seconds=2.0,
        elapsed_seconds=864000,
        last_clean_merkle_seconds=300,
        network_throughput_gbps=1.0,
        network_std_gbps=0.1,
        anomaly_score=0.1,
    )
    snapshot = monitor.evaluate_once(state)

    rec_color = {"PROCEED": "green", "CAUTION": "yellow", "BLOCK": "red"}.get(
        snapshot.recommendation, "white"
    )

    console.print(Panel(
        f"PONR P5:  [cyan]${snapshot.p5_usd:,.0f}[/cyan]\n"
        f"PONR P50: [cyan]${snapshot.p50_usd:,.0f}[/cyan]\n"
        f"PONR P95: [cyan]${snapshot.p95_usd:,.0f}[/cyan] "
        f"{'[green](BELOW SLA)[/green]' if snapshot.rollback_feasible else '[red](ABOVE SLA)[/red]'}\n"
        f"Replication lag: [cyan]{snapshot.replication_lag_seconds:.1f}s[/cyan]\n"
        f"Anomaly score: [cyan]{snapshot.anomaly_score:.4f}[/cyan]\n"
        f"Network health: [cyan]{snapshot.network_health_index:.2f}[/cyan]\n"
        f"Event horizon: [cyan]{snapshot.event_horizon_seconds}s[/cyan]\n\n"
        f"System recommendation: [{rec_color}]{snapshot.recommendation}[/{rec_color}]",
        title="Final Call — PONR Status",
    ))

    console.print(Panel(
        "Operator has 25 minutes to respond:\n\n"
        "  [green]GO[/green]       python main.py execute --token <jwt>\n"
        "  [yellow]POSTPONE[/yellow]  python main.py postpone --reason <code>\n"
        "  [red]ABORT[/red]     python main.py abort --reason <reason>\n\n"
        "No response within 25 minutes = automatic POSTPONE + IC paged.",
        title="Operator Decision Required",
    ))


@app.command()
def execute(
    token: str = typer.Option(..., help="Signed operator JWT token"),
    operator: str = typer.Option("operator", help="Operator email for audit trail"),
):
    """Execute the atomic cutover sequence."""
    from src.override.gate import OperatorOverrideGate
    from src.cutover.sequence import AtomicCutoverSequence, SequenceState

    console.rule("[bold red]Phase 5 — ATOMIC CUTOVER SEQUENCE")

    gate = OperatorOverrideGate()
    decision = gate.submit_go(token)

    if decision.decision.value != "go":
        console.print(f"[red]GO REJECTED: {decision.reason}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]GO ACCEPTED — operator={decision.operator}[/green]")
    console.print("[bold red]CUTOVER SEQUENCE STARTING IN 3 SECONDS...[/bold red]")
    import time; time.sleep(3)

    sequence = AtomicCutoverSequence(
        source_pool=None,
        target_pool=None,
    )

    with console.status("Executing cutover sequence..."):
        result = sequence.execute(operator=operator, token_id=decision.token_id or "")

    table = Table(title=f"Cutover Result: {result.state.value.upper()}")
    table.add_column("Step", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Duration", style="white")
    table.add_column("Result", style="white")

    for step in result.steps:
        status = "[green]✓[/green]" if step.success else "[red]✗[/red]"
        table.add_row(
            str(step.step_number),
            step.name,
            f"{step.duration_ms:.0f}ms",
            status,
        )

    console.print(table)
    console.print(Panel(
        f"State: [{'green' if result.state == SequenceState.COMPLETED else 'red'}]{result.state.value}[/]\n"
        f"Total duration: [cyan]{result.total_duration_ms:.0f}ms[/cyan]\n"
        f"Fencing epoch: [cyan]{result.fencing_epoch}[/cyan]\n"
        f"Post-Merkle clean: [{'green' if result.post_cutover_merkle_clean else 'red'}]"
        f"{result.post_cutover_merkle_clean}[/]",
        title="Cutover Summary",
    ))

    if result.state == SequenceState.COMPLETED:
        console.print("\n[green]MIGRATION COMPLETE — Cloud target is now authoritative[/green]")
        console.print("[green]Edge source is read-only. Phase 6 can begin after 24h validation.[/green]")
    elif result.state == SequenceState.ABORTED:
        console.print(f"\n[red]ABORT at Step {result.abort_at_step}: {result.abort_reason.value}[/red]")
        console.print("[red]Edge source remains authoritative. Review logs before retrying.[/red]")
    else:
        console.print("\n[red]Post-Merkle failed — Tier 4 rollback protocol required[/red]")
        raise typer.Exit(1)


@app.command()
def abort(reason: str = typer.Option(..., help="Abort reason"), operator: str = typer.Option("operator")):
    """Abort the migration."""
    from src.override.gate import OperatorOverrideGate
    gate = OperatorOverrideGate()
    decision = gate.submit_abort(operator, reason)
    console.print(f"[red]MIGRATION ABORTED: {decision.reason}[/red]")
    console.print("[red]Post-mortem required within 24 hours before re-migration.[/red]")


@app.command()
def postpone(reason: str = typer.Option(..., help="Reason code"), operator: str = typer.Option("operator")):
    """Postpone cutover with 4-hour cooldown."""
    from src.override.gate import OperatorOverrideGate
    gate = OperatorOverrideGate()
    decision = gate.submit_postpone(operator, reason)
    console.print(f"[yellow]POSTPONED: {decision.reason}[/yellow]")
    console.print("[yellow]Pre-flight checklist re-evaluated in 4 hours.[/yellow]")


@app.command(name="generate-test-token")
def generate_test_token(operator: str = typer.Option("test-operator")):
    """Generate a test JWT token for development environments."""
    from src.override.gate import OperatorOverrideGate
    gate = OperatorOverrideGate()
    token = gate.generate_test_token(operator=operator, ponr_p95=500.0, lag=2.0, anomaly=0.1)
    console.print(Panel(token, title="Test Token (NOT for production)"))
    console.print("[yellow]Use with: python main.py execute --token <token>[/yellow]")


if __name__ == "__main__":
    app()
