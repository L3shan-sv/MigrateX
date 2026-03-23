"""
main.py — Phase 6: Post-Migration Hardening
"""

from __future__ import annotations
import json
import logging
import typer
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

app = typer.Typer(name="migratex-phase6", help="MigrateX Phase 6 — Post-Migration Hardening")
console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")


@app.command()
def decommission(
    days_since_cutover: float = typer.Option(0.0, help="Days since cutover (for testing)"),
):
    """Show decommission status and checklist."""
    from src.decommission.coordinator import DecommissionCoordinator
    from datetime import timedelta

    console.rule("[bold blue]Phase 6 — Edge Decommission Status")
    cutover_at = (datetime.utcnow() - timedelta(days=days_since_cutover)).isoformat()
    coord = DecommissionCoordinator(cutover_completed_at=cutover_at)

    status = coord.get_status()
    console.print(Panel(
        f"Current phase: [cyan]{status.current_day.value}[/cyan]\n"
        f"Days elapsed: [cyan]{status.days_elapsed:.1f}[/cyan]\n"
        f"Write attempts: [{'red' if status.write_attempts else 'green'}]{len(status.write_attempts)}[/]\n"
        f"Unresolved orphans: [{'red' if status.orphans_unresolved else 'green'}]{status.orphans_unresolved}[/]\n"
        f"Backup completed: [{'green' if status.backup_completed else 'red'}]{status.backup_completed}[/]\n"
        f"Binlog archived: [{'green' if status.binlog_archived else 'red'}]{status.binlog_archived}[/]\n"
        f"Deletion lock: [yellow]{status.deletion_lock_active}[/yellow]\n"
        f"Safe to terminate: [{'green' if status.safe_to_terminate else 'red'}]{status.safe_to_terminate}[/]",
        title="Decommission Status",
    ))

    table = Table(title="Decommission Checklist")
    table.add_column("Day", style="cyan")
    table.add_column("Action", style="white")
    table.add_column("Status", style="white")
    for item in coord.generate_checklist():
        color = {"COMPLETE": "green", "BLOCKING": "red", "PENDING": "dim", "SAFE": "green"}.get(
            item["status"], "yellow"
        )
        table.add_row(item["day"], item["action"], f"[{color}]{item['status']}[/{color}]")
    console.print(table)


@app.command()
def retrain(
    output: str = typer.Option("./artifacts/model", help="Output directory"),
    samples: int = typer.Option(2016, help="Number of training samples"),
):
    """Retrain Isolation Forest on cloud-native baseline."""
    from src.retraining.engine import ModelRetrainingEngine
    console.rule("[bold blue]Phase 6 — Model Retraining")

    engine = ModelRetrainingEngine(output_dir=output)
    with console.status("Generating synthetic cloud-native baseline..."):
        signals = engine.generate_synthetic_cloud_signals(n_samples=samples)
    with console.status("Retraining Isolation Forest..."):
        result = engine.retrain_isolation_forest(signals)

    console.print(Panel(
        f"Training samples: [cyan]{result.training_samples}[/cyan]\n"
        f"New threshold: [cyan]{result.new_threshold}[/cyan]\n"
        f"Threshold change: [cyan]{result.threshold_change_pct:.1f}%[/cyan]\n"
        f"Calibration: [{'green' if result.calibration_passed else 'red'}]"
        f"{'PASS' if result.calibration_passed else 'FAIL'}[/]\n"
        f"Saved to: [cyan]{result.saved_to}[/cyan]",
        title="Retraining Result",
    ))

    with console.status("Recalibrating PONR for cloud-internal operations..."):
        ponr = engine.recalibrate_ponr(
            observed_cloud_writes_per_sec=50_000_000,
            observed_cloud_network_gbps=10.0,
            observed_cloud_network_std_gbps=0.5,
        )

    console.print(Panel(
        f"Old network mean: [cyan]{ponr.old_network_mean_gbps:.2f} Gbps[/cyan]\n"
        f"New network mean: [cyan]{ponr.new_network_mean_gbps:.2f} Gbps[/cyan]\n"
        f"P50 deviation: [cyan]{ponr.p50_deviation_pct:.1f}%[/cyan]\n"
        f"Calibration: [{'green' if ponr.calibration_passed else 'red'}]"
        f"{'PASS' if ponr.calibration_passed else 'FAIL'}[/]",
        title="PONR Recalibration",
    ))


@app.command()
def report(
    migration_id: str = typer.Option("mig-001", help="Migration ID"),
    output: str = typer.Option("./artifacts/reports", help="Output directory"),
    downtime_ms: float = typer.Option(87.0, help="Actual cutover downtime in ms"),
    actual_cost: float = typer.Option(450.0, help="Actual egress cost in USD"),
):
    """Generate the SRE post-mortem report."""
    from src.report.generator import SREReportGenerator
    console.rule("[bold blue]Phase 6 — SRE Post-Mortem Report")

    gen = SREReportGenerator(output_dir=output)
    with console.status("Generating report..."):
        r = gen.generate(
            migration_id=migration_id,
            started_at="2026-01-01T00:00:00",
            cutover_completed_at=datetime.utcnow().isoformat(),
            cutover_result={"ponr_p95_usd": 800.0},
            actual_cost_usd=actual_cost,
            downtime_ms=downtime_ms,
            data_volume_gb=100.0,
        )

    console.print(Panel(
        f"Report ID: [cyan]{r.report_id}[/cyan]\n"
        f"Duration: [cyan]{r.summary.total_duration_days:.1f} days[/cyan]\n"
        f"Downtime: [cyan]{r.summary.application_downtime_ms:.0f}ms[/cyan]\n"
        f"Data loss: [green]{r.summary.data_loss_events} events[/green]\n"
        f"Cost: [cyan]${r.summary.final_egress_cost_usd:,.2f}[/cyan]\n"
        f"Resilience score: [cyan]{r.summary.resilience_score}/100[/cyan]",
        title="Migration Summary",
    ))

    console.print("\n[bold]Lessons learned:[/bold]")
    for lesson in r.lessons_learned:
        console.print(f"  • {lesson}")


@app.command()
def runbooks(output: str = typer.Option("./artifacts/runbooks", help="Output directory")):
    """Generate all 4 operational runbooks."""
    from src.runbooks.generator import RunbookGenerator
    console.rule("[bold blue]Phase 6 — Runbook Generation")

    gen = RunbookGenerator(output_dir=output)
    with console.status("Generating runbooks..."):
        rbs = gen.generate_all()

    for rb in rbs:
        console.print(f"  [green]✓[/green] {rb.runbook_id}: {rb.title}")
    console.print(f"\n[green]{len(rbs)} runbooks written → {output}[/green]")


@app.command()
def run(output: str = typer.Option("./artifacts", help="Base output directory")):
    """Run the full Phase 6 pipeline."""
    console.rule("[bold blue]MigrateX Phase 6 — Post-Migration Hardening")

    console.print("\n[bold]Step 1/4: Edge decommission status[/bold]")
    decommission(days_since_cutover=1.0)

    console.print("\n[bold]Step 2/4: Model retraining[/bold]")
    retrain(output=f"{output}/model")

    console.print("\n[bold]Step 3/4: SRE post-mortem report[/bold]")
    report(output=f"{output}/reports")

    console.print("\n[bold]Step 4/4: Operational runbooks[/bold]")
    runbooks(output=f"{output}/runbooks")

    console.rule("[bold green]MigrateX — Migration Complete")
    console.print("\n[green]Cloud target is authoritative.[/green]")
    console.print("[green]Models retrained on cloud-native baseline.[/green]")
    console.print("[green]SRE report published.[/green]")
    console.print("[green]Runbooks delivered to platform team.[/green]")
    console.print("[yellow]Edge source decommission: follow 10-day protocol.[/yellow]")


if __name__ == "__main__":
    app()
