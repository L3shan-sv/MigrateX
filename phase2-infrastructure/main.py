"""
main.py
───────
Phase 2 CLI entrypoint — Infrastructure Provisioning.

Commands:
  provision          Full provisioning pipeline (topics + connector + etcd check)
  check-binlog       Verify binlog config on source MySQL
  create-topics      Create Kafka topics for all tables in schema inventory
  register-connector Register Debezium connector with Kafka Connect
  check-lease        Verify etcd cluster health
  start-sink         Start the sink consumer (long-running)
  validate-synthetic Run synthetic load validation protocol
  status             Show current infrastructure status
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

app = typer.Typer(name="migratex-phase2", help="MigrateX Phase 2 — Infrastructure Provisioning")
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("migratex.phase2")


def _settings():
    from src.config.settings import settings
    return settings


@app.command(name="check-binlog")
def check_binlog():
    """Verify MySQL binlog configuration on source database."""
    from src.cdc.connector import DebeziumConnectorManager
    from src.db import DBPool

    console.rule("[bold blue]Phase 2 — Binlog Configuration Check")
    s = _settings()

    pool = DBPool(
        host=s.source_host, port=s.source_port,
        user=s.source_user, password=s.source_password,
        database=s.source_database, pool_name="binlog_check"
    )

    manager = DebeziumConnectorManager(
        connect_url=s.kafka_connect_url,
        source_host=s.source_host,
        source_port=s.source_port,
        source_user=s.source_user,
        source_password=s.source_password,
        source_database=s.source_database,
        topic_prefix=s.kafka_topic_prefix,
        kafka_bootstrap=s.kafka_bootstrap_servers,
    )

    checks = manager.verify_binlog_config(pool)

    table = Table(title="Binlog Configuration Check")
    table.add_column("Setting", style="cyan")
    table.add_column("Status", style="white")

    all_pass = True
    for setting, passed in checks.items():
        status = "[green]PASS[/green]" if passed else "[red]FAIL[/red]"
        table.add_row(setting, status)
        if not passed:
            all_pass = False

    console.print(table)

    if all_pass:
        console.print("\n[green]All binlog checks passed. Ready to register connector.[/green]")
    else:
        console.print("\n[red]Binlog check FAILED. Fix the above before proceeding.[/red]")
        raise typer.Exit(1)


@app.command(name="create-topics")
def create_topics(
    inventory: str = typer.Option("./artifacts/schema_inventory.json", help="Path to schema inventory"),
):
    """Create Kafka topics for all tables from Phase 1 schema inventory."""
    from src.cdc.topics import KafkaTopicManager

    console.rule("[bold blue]Phase 2 — Kafka Topic Creation")
    s = _settings()

    if not Path(inventory).exists():
        console.print(f"[red]Schema inventory not found: {inventory}[/red]")
        console.print("[red]Run Phase 1 first: python main.py scan[/red]")
        raise typer.Exit(1)

    with open(inventory) as f:
        inv = json.load(f)

    tables = [t["table_name"] for t in inv.get("tables", [])]
    database = inv.get("database", s.source_database)

    console.print(f"Creating {len(tables)} topics + system topics...")

    mgr = KafkaTopicManager(
        bootstrap_servers=s.kafka_bootstrap_list,
        topic_prefix=s.kafka_topic_prefix,
        partitions=s.kafka_partitions,
        replication_factor=s.kafka_replication_factor,
        retention_ms=s.kafka_retention_ms,
    )

    with console.status("Creating system topics..."):
        mgr.create_system_topics()

    with console.status(f"Creating {len(tables)} table topics..."):
        results = mgr.create_table_topics(database, tables)

    created = sum(1 for v in results.values() if v)
    table = Table(title="Topic Creation Results")
    table.add_column("Result", style="cyan")
    table.add_column("Count", style="white")
    table.add_row("Topics created/verified", str(created))
    table.add_row("Topics failed", str(len(results) - created))
    table.add_row("Partitions per topic", str(s.kafka_partitions))
    table.add_row("Retention", "7 days (604,800,000 ms)")
    table.add_row("Compression", "LZ4")
    console.print(table)
    mgr.close()


@app.command(name="register-connector")
def register_connector():
    """Register Debezium MySQL connector with Kafka Connect."""
    from src.cdc.connector import DebeziumConnectorManager

    console.rule("[bold blue]Phase 2 — Debezium Connector Registration")
    s = _settings()

    mgr = DebeziumConnectorManager(
        connect_url=s.kafka_connect_url,
        source_host=s.source_host,
        source_port=s.source_port,
        source_user=s.source_user,
        source_password=s.source_password,
        source_database=s.source_database,
        topic_prefix=s.kafka_topic_prefix,
        kafka_bootstrap=s.kafka_bootstrap_servers,
        connector_name=s.debezium_connector_name,
    )

    with console.status("Registering connector..."):
        success = mgr.register()

    if not success:
        console.print("[red]Connector registration failed.[/red]")
        raise typer.Exit(1)

    console.print("[green]Connector registered. Waiting for RUNNING state...[/green]")

    with console.status("Waiting for connector to reach RUNNING state..."):
        running = mgr.wait_for_running(timeout_seconds=120)

    if running:
        status = mgr.get_status()
        console.print(Panel(
            f"Connector: [cyan]{status.name}[/cyan]\n"
            f"State: [green]{status.state}[/green]\n"
            f"CDC lag: [cyan]{status.lag_seconds:.1f}s[/cyan]\n"
            f"GTID position: [cyan]{status.gtid_position or 'reading...'}[/cyan]",
            title="Connector Status",
        ))
    else:
        console.print("[red]Connector failed to reach RUNNING state.[/red]")
        raise typer.Exit(1)


@app.command(name="check-lease")
def check_lease():
    """Verify etcd cluster health and fencing epoch."""
    from src.lease.fencing import FencingTokenClient, Authority

    console.rule("[bold blue]Phase 2 — etcd Lease Check")
    s = _settings()

    console.print(f"etcd endpoints: {s.etcd_endpoints}")

    try:
        client = FencingTokenClient(
            endpoints=s.etcd_endpoints_list,
            authority=Authority.EDGE,
            lease_ttl_seconds=s.etcd_lease_ttl_seconds,
        )
        token = client.get_current_token()
        if token:
            console.print(Panel(
                f"Current authority: [cyan]{token.authority.value}[/cyan]\n"
                f"Epoch: [cyan]{token.epoch}[/cyan]\n"
                f"Acquired at: [cyan]{token.acquired_at}[/cyan]",
                title="Current Fencing Token",
            ))
        else:
            console.print("[yellow]No active fencing token — cluster ready for first lease acquisition[/yellow]")
        console.print("[green]etcd cluster reachable[/green]")
    except Exception as e:
        console.print(f"[red]etcd check failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def provision(
    inventory: str = typer.Option("./artifacts/schema_inventory.json"),
    skip_binlog: bool = typer.Option(False, help="Skip binlog check (use in dev/test)"),
):
    """Run the full Phase 2 provisioning pipeline."""
    console.rule("[bold blue]MigrateX Phase 2 — Full Infrastructure Provisioning")

    if not skip_binlog:
        console.print("\n[bold]Step 1/4: Binlog configuration check[/bold]")
        check_binlog()

    console.print("\n[bold]Step 2/4: Kafka topic creation[/bold]")
    create_topics(inventory=inventory)

    console.print("\n[bold]Step 3/4: Debezium connector registration[/bold]")
    register_connector()

    console.print("\n[bold]Step 4/4: etcd lease check[/bold]")
    check_lease()

    console.rule("[bold green]Phase 2 Provisioning Complete")
    console.print("\nExit criteria to verify before Phase 3:")
    console.print("  [ ] CDC consumer lag < 30s (sustained 4 hours)")
    console.print("  [ ] All Kafka topics created with 7-day retention")
    console.print("  [ ] Debezium connector in RUNNING state")
    console.print("  [ ] etcd cluster: all 3 nodes in quorum")
    console.print("  [ ] Synthetic validation protocol passed")
    console.print("\n[green]Run 'python main.py validate-synthetic' to complete exit criteria.[/green]")


@app.command(name="validate-synthetic")
def validate_synthetic():
    """Run synthetic load validation — Phase 2 exit criterion."""
    console.rule("[bold blue]Phase 2 — Synthetic Validation Protocol")
    console.print("This validates the full pipeline on synthetic data before production contact.")
    console.print("\nValidation steps:")
    console.print("  1. Generate synthetic dataset at 10% of production volume")
    console.print("  2. Load to staging source replica")
    console.print("  3. Run Debezium snapshot — verify row counts within 0.001%")
    console.print("  4. Run 1x write workload for 1 hour — verify lag < 30s")
    console.print("  5. Inject 3 network partitions (5-min each) — verify recovery")
    console.print("  6. Run idempotency test — verify zero double writes")
    console.print("  7. Verify all 7 alert rules fire correctly")
    console.print("\n[yellow]Full synthetic validation requires live infrastructure.[/yellow]")
    console.print("[yellow]Connect Phase 2 to Docker Compose stack and re-run.[/yellow]")


if __name__ == "__main__":
    app()
