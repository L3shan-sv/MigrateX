"""
main.py
───────
Phase 1 CLI entrypoint.
Runs the full discovery pipeline and produces all artifacts.

Usage:
    python main.py run              # full discovery pipeline
    python main.py scan             # schema scan only
    python main.py profile          # traffic profile only
    python main.py ponr-estimate    # PONR pre-migration estimate
    python main.py train-baseline   # train anomaly detector (synthetic data)
    python main.py api              # start the REST API
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

app = typer.Typer(name="migratex-phase1", help="MigrateX Phase 1 — Discovery & Risk Profiling")
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("migratex.phase1")


def _get_settings():
    from src.settings import settings
    return settings


def _get_source_pool(settings):
    from src.db import DBPool
    return DBPool(
        host=settings.source_host,
        port=settings.source_port,
        user=settings.source_user,
        password=settings.source_password,
        database=settings.source_database,
        pool_name="source_pool",
    )


@app.command()
def scan(
    output: str = typer.Option("./artifacts", help="Output directory for artifacts"),
):
    """Run schema inventory scan against source database."""
    settings = _get_settings()
    output_dir = Path(output)

    console.rule("[bold blue]MigrateX Phase 1 — Schema Scan")

    with console.status("Connecting to source database..."):
        pool = _get_source_pool(settings)
        if not pool.ping():
            console.print("[red]ERROR: Cannot connect to source database. Check your .env config.[/red]")
            raise typer.Exit(1)

    console.print(f"[green]Connected → {settings.source_host}:{settings.source_port}/{settings.source_database}[/green]")

    from src.scanner.schema import SchemaScanner, save_inventory
    with console.status("Scanning schema..."):
        scanner = SchemaScanner(pool, settings.source_database)
        inventory = scanner.run()

    save_inventory(inventory, output_dir)

    table = Table(title="Schema Inventory Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    table.add_row("Tables", str(len(inventory.tables)))
    table.add_row("Total data", f"{inventory.total_data_bytes / 1e9:.2f} GB")
    table.add_row("Total rows (estimate)", f"{inventory.total_row_count:,}")
    table.add_row("Stored procedures", str(len(inventory.stored_procedures)))
    table.add_row("Triggers", str(len(inventory.triggers)))
    table.add_row("Views", str(len(inventory.views)))

    pii_tables = sum(
        1 for t in inventory.tables
        if any(c.is_pii_candidate for c in t.columns)
    )
    table.add_row("Tables with PII candidates", str(pii_tables))
    table.add_row("FK dependency edges", str(len(inventory.fk_dependency_edges)))

    console.print(table)
    console.print(f"\n[green]Artifacts written → {output_dir}[/green]")


@app.command()
def profile(
    output: str = typer.Option("./artifacts", help="Output directory for artifacts"),
):
    """Run traffic profile analysis against performance_schema."""
    settings = _get_settings()
    output_dir = Path(output)

    console.rule("[bold blue]MigrateX Phase 1 — Traffic Profiler")

    with console.status("Connecting..."):
        pool = _get_source_pool(settings)
        if not pool.ping():
            console.print("[red]ERROR: Cannot connect to source database.[/red]")
            raise typer.Exit(1)

    from src.profiler.traffic import TrafficProfiler, save_traffic_profile
    with console.status("Profiling traffic patterns..."):
        profiler = TrafficProfiler(pool, settings.source_database)
        traffic = profiler.run()

    save_traffic_profile(traffic, output_dir)

    table = Table(title="Traffic Profile Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    table.add_row("Global SELECT QPS", f"{traffic.global_select_qps:.1f}")
    table.add_row("Global write QPS", f"{traffic.global_write_qps:.1f}")
    table.add_row("Peak total QPS", f"{traffic.peak_total_qps:.1f}")
    table.add_row("Write amplification factor", f"{traffic.write_amplification_factor:.2f}x")
    table.add_row("Batch write ratio", f"{traffic.batch_write_ratio * 100:.1f}%")
    if traffic.connection_stats:
        table.add_row("Connection pool utilisation", f"{traffic.connection_stats.utilisation_pct:.1f}%")
    console.print(table)
    console.print(f"\n[green]Artifacts written → {output_dir}[/green]")


@app.command(name="ponr-estimate")
def ponr_estimate(
    data_gb: float = typer.Option(..., help="Total data volume in GB"),
    write_rate_mbps: float = typer.Option(..., help="Source write rate in MB/s"),
    network_mean_gbps: float = typer.Option(..., help="Network throughput mean in Gbps"),
    network_std_gbps: float = typer.Option(0.1, help="Network throughput std deviation in Gbps"),
    sla_usd: float = typer.Option(5000.0, help="Rollback cost SLA threshold in USD"),
    output: str = typer.Option("./artifacts", help="Output directory"),
):
    """Run Monte Carlo PONR pre-migration estimate."""
    from src.ponr.engine import PONREngine, NetworkProfile, save_ponr_estimate

    console.rule("[bold blue]MigrateX Phase 1 — PONR Pre-Migration Estimate")
    console.print(f"Data volume: [cyan]{data_gb:.1f} GB[/cyan]")
    console.print(f"Write rate: [cyan]{write_rate_mbps:.1f} MB/s[/cyan]")
    console.print(f"Network mean: [cyan]{network_mean_gbps:.2f} Gbps[/cyan]")
    console.print(f"SLA threshold: [cyan]${sla_usd:,.0f}[/cyan]")

    engine = PONREngine(
        n_simulations=10_000,
        sla_threshold_usd=sla_usd,
    )
    network = NetworkProfile(
        mean_throughput_gbps=network_mean_gbps,
        std_throughput_gbps=network_std_gbps,
        rtt_ms_p50=5.0,
        rtt_ms_p99=25.0,
        packet_loss_pct=0.01,
    )

    with console.status("Running 10,000 Monte Carlo simulations..."):
        estimate = engine.run_pre_migration_estimate(
            total_data_bytes=int(data_gb * 1e9),
            write_rate_bytes_per_sec=write_rate_mbps * 1e6,
            network=network,
        )

    output_dir = Path(output)
    save_ponr_estimate(estimate, output_dir)

    table = Table(title="PONR Pre-Migration Estimate")
    table.add_column("Migration progress", style="cyan")
    table.add_column("P50 rollback cost", style="white")
    table.add_column("P95 rollback cost", style="white")
    table.add_column("Feasible?", style="white")
    table.add_column("Recommendation", style="white")

    for row in estimate["rollback_cost_by_progress"]:
        feasible = "[green]YES[/green]" if row["rollback_feasible"] else "[red]NO[/red]"
        rec_color = {"PROCEED": "green", "CAUTION": "yellow", "BLOCK": "red"}.get(
            row["recommendation"], "white"
        )
        table.add_row(
            f"{row['progress_pct']:.0f}%",
            f"${row['p50_usd']:,.0f}",
            f"${row['p95_usd']:,.0f}",
            feasible,
            f"[{rec_color}]{row['recommendation']}[/{rec_color}]",
        )

    console.print(table)
    console.print(Panel(
        f"Estimated duration — P50: [cyan]{estimate['estimated_duration_hours']['p50']:.1f}h[/cyan]  "
        f"P95: [cyan]{estimate['estimated_duration_hours']['p95']:.1f}h[/cyan]\n"
        f"SLA threshold: [cyan]${estimate['sla_threshold_usd']:,.0f}[/cyan]",
        title="Summary",
    ))
    console.print(f"\n[green]Estimate saved → {output_dir}/ponr_phase1_estimate.json[/green]")


@app.command(name="train-baseline")
def train_baseline(
    output: str = typer.Option("./artifacts/model", help="Output directory for trained model"),
    synthetic: bool = typer.Option(True, help="Use synthetic data for testing"),
    samples: int = typer.Option(2016, help="Number of training samples (min 2016 for 14 days)"),
):
    """Train Isolation Forest anomaly detector on baseline data."""
    from src.ml_baseline.anomaly import AnomalyDetector, generate_synthetic_baseline

    console.rule("[bold blue]MigrateX Phase 1 — Anomaly Detector Training")
    output_dir = Path(output)

    if synthetic:
        console.print(f"[yellow]Using synthetic baseline data ({samples} samples)[/yellow]")
        console.print("[yellow]In production: replace with 14-day real signal collection[/yellow]")
        with console.status("Generating synthetic baseline..."):
            signals = generate_synthetic_baseline(n_samples=samples)
    else:
        console.print("[red]Real signal collection not yet implemented.[/red]")
        console.print("[red]Use --synthetic flag for testing.[/red]")
        raise typer.Exit(1)

    detector = AnomalyDetector(
        contamination=0.05,
        alert_threshold=0.7,
    )

    with console.status("Training Isolation Forest..."):
        metadata = detector.fit(signals)

    detector.save(output_dir)

    table = Table(title="Model Training Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    table.add_row("Training samples", str(metadata.training_samples))
    table.add_row("Contamination rate", str(metadata.contamination))
    table.add_row("Alert threshold", str(metadata.alert_threshold))
    table.add_row("Features", str(len(metadata.feature_names)))
    for cal in metadata.calibration_results:
        table.add_row(
            f"Calibration: {cal['check']}",
            f"{'PASS' if cal['passed'] else 'FAIL'} "
            f"(expected={cal['expected_anomalous']}, actual={cal['actual_flagged']})"
        )

    console.print(table)
    console.print(f"\n[green]Model saved → {output_dir}[/green]")


@app.command()
def run(
    output: str = typer.Option("./artifacts", help="Output directory for all artifacts"),
    ponr_data_gb: float = typer.Option(100.0, help="Estimated data volume in GB for PONR"),
    ponr_write_mbps: float = typer.Option(50.0, help="Write rate in MB/s for PONR"),
    ponr_network_gbps: float = typer.Option(1.0, help="Network throughput in Gbps"),
    ponr_sla_usd: float = typer.Option(5000.0, help="Rollback SLA threshold USD"),
):
    """Run the full Phase 1 discovery pipeline end-to-end."""
    console.rule("[bold blue]MigrateX Phase 1 — Full Discovery Pipeline")
    console.print("This will run: schema scan → traffic profile → PONR estimate → model training\n")

    typer.echo("Step 1/4: Schema scan")
    scan(output=output)

    typer.echo("\nStep 2/4: Traffic profile")
    profile(output=output)

    typer.echo("\nStep 3/4: PONR pre-migration estimate")
    ponr_estimate(
        data_gb=ponr_data_gb,
        write_rate_mbps=ponr_write_mbps,
        network_mean_gbps=ponr_network_gbps,
        network_std_gbps=0.1,
        sla_usd=ponr_sla_usd,
        output=output,
    )

    typer.echo("\nStep 4/4: Anomaly detector training (synthetic baseline)")
    train_baseline(output=f"{output}/model", synthetic=True)

    console.rule("[bold green]Phase 1 Complete")
    console.print(f"All artifacts written to: [cyan]{output}[/cyan]")
    console.print("\nArtifacts produced:")
    console.print("  schema_inventory.json")
    console.print("  fk_dependency_graph.json")
    console.print("  pii_surface_map.json")
    console.print("  traffic_profile.json")
    console.print("  ponr_phase1_estimate.json")
    console.print("  model/isolation_forest.pkl")
    console.print("  model/model_metadata.json")
    console.print("\n[green]Phase 1 exit criteria can now be reviewed.[/green]")


if __name__ == "__main__":
    app()
