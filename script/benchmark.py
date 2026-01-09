#!/usr/bin/env python3
"""
Benchmark script for ikada Raft cluster.

Runs memtier_benchmark multiple times and generates statistics, CSV output,
and optional graphs using matplotlib.
"""

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from statistics import mean, stdev

import structlog
import matplotlib.pyplot as plt
import numpy as np


def get_git_info() -> dict:
    """Get current git branch and commit hash."""
    info = {"branch": "unknown", "commit": "unknown"}
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        info["branch"] = result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        info["commit"] = result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    return info


def setup_run_logger(output_dir: Path, run_number: int):
    """Setup a structured logger for a single benchmark run."""
    log_file = output_dir / f"run_{run_number}.log"
    log_file_handle = open(log_file, 'w')

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.WriteLoggerFactory(file=log_file_handle),
        cache_logger_on_first_use=False,
    )
    return structlog.get_logger(), log_file_handle


def run_benchmark(
    host: str,
    port: int,
    threads: int,
    clients: int,
    test_time: int,
    ratio: str,
    timeout: int,
    output_dir: Path,
    run_number: int,
) -> dict:
    """Run a single memtier_benchmark and return parsed results."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        json_file = f.name

    start_time = datetime.now()

    cmd = [
        "memtier_benchmark",
        "--protocol=memcache_text",
        f"--server={host}",
        f"--port={port}",
        f"--threads={threads}",
        f"--clients={clients}",
        f"--test-time={test_time}",
        f"--ratio={ratio}",
        "--hide-histogram",
        f"--json-out-file={json_file}",
    ]

    logger, log_file_handle = setup_run_logger(output_dir, run_number)

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            "benchmark_completed",
            run_number=run_number,
            status="SUCCESS",
            exit_code=0,
            duration=round(duration, 1),
            command=' '.join(cmd),
            timeout=timeout,
            stdout=result.stdout if result.stdout else "",
            stderr=result.stderr if result.stderr else "",
        )

        with open(json_file) as f:
            data = json.load(f)

        totals = data["ALL STATS"]["Totals"]
        sets_data = data["ALL STATS"]["Sets"]
        gets_data = data["ALL STATS"]["Gets"]
        percentiles = totals.get("Percentile Latencies", {})

        return {
            "total_ops": totals["Ops/sec"],
            "set_ops": sets_data["Ops/sec"],
            "get_ops": gets_data["Ops/sec"],
            "avg_latency": totals.get("Average Latency", totals.get("Latency", 0)),
            "p50_latency": percentiles.get("p50.00", 0),
            "p99_latency": percentiles.get("p99.00", 0),
            "p999_latency": percentiles.get("p99.90", 0),
            "kb_sec": totals["KB/sec"],
        }

    except subprocess.TimeoutExpired as e:
        duration = (datetime.now() - start_time).total_seconds()
        stdout = e.stdout.decode() if e.stdout else ""
        stderr = e.stderr.decode() if e.stderr else ""

        logger.warning(
            "benchmark_timeout",
            run_number=run_number,
            status="TIMEOUT",
            exit_code=-1,
            duration=round(duration, 1),
            command=' '.join(cmd),
            timeout=timeout,
            error=f"Command timed out after {timeout} seconds",
            stdout=stdout,
            stderr=stderr,
        )

        raise TimeoutError(f"Benchmark timed out after {timeout}s")

    except subprocess.CalledProcessError as e:
        duration = (datetime.now() - start_time).total_seconds()

        logger.error(
            "benchmark_failed",
            run_number=run_number,
            status="FAILED",
            exit_code=e.returncode,
            duration=round(duration, 1),
            command=' '.join(cmd),
            timeout=timeout,
            error=str(e),
            stdout=e.stdout if e.stdout else "",
            stderr=e.stderr if e.stderr else "",
        )

        raise

    except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
        duration = (datetime.now() - start_time).total_seconds()

        logger.error(
            "benchmark_parse_error",
            run_number=run_number,
            status="FAILED",
            exit_code=-1,
            duration=round(duration, 1),
            command=' '.join(cmd),
            timeout=timeout,
            error=f"Failed to parse results: {e}",
            stdout="",
            stderr="",
        )

        raise

    finally:
        if os.path.exists(json_file):
            os.unlink(json_file)

        log_file_handle.close()


def calculate_stats(results: list[dict]) -> dict:
    """Calculate statistics from multiple benchmark runs."""
    if not results:
        return {}

    metrics = [
        "total_ops",
        "set_ops",
        "get_ops",
        "avg_latency",
        "p50_latency",
        "p99_latency",
        "p999_latency",
        "kb_sec",
    ]

    stats = {}
    for metric in metrics:
        values = [r[metric] for r in results]
        avg_value = sum(values) / len(values)
        stats[metric] = {
            "avg": avg_value,
            "min": min(values),
            "max": max(values),
            "stdev": stdev(values) if len(values) > 1 else 0,
        }

    return stats


def write_csv(results: list[dict], filepath: Path) -> None:
    """Write benchmark results to CSV file."""
    fieldnames = [
        "run",
        "timestamp",
        "total_ops",
        "set_ops",
        "get_ops",
        "avg_latency",
        "p50_latency",
        "p99_latency",
        "p999_latency",
        "kb_sec",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i, result in enumerate(results, 1):
            row = {"run": i, "timestamp": result.get("timestamp", "")}
            row.update({k: result[k] for k in fieldnames if k in result})
            writer.writerow(row)


def generate_graphs(results: list[dict], stats: dict, output_dir: Path) -> None:
    """Generate benchmark graphs using matplotlib."""
    # Create a single figure with 3 subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # Metrics to plot
    metrics = [
        ("total_ops", "Total Throughput", "ops/sec", "#2ecc71"),
        ("avg_latency", "Average Latency", "ms", "#9b59b6"),
        ("p99_latency", "p99 Latency", "ms", "#e74c3c")
    ]

    # Find max value across all latency metrics for consistent y-axis
    latency_metrics = ["avg_latency", "p99_latency"]
    max_latency = max([stats[m]["max"] for m in latency_metrics if m in stats])

    for idx, (metric, title, unit, color) in enumerate(metrics):
        ax = axes[idx]
        values = [r[metric] for r in results]
        mean_val = stats[metric]["avg"]
        min_val = stats[metric]["min"]
        max_val = stats[metric]["max"]

        # Calculate error bars (distance from mean to min/max)
        error_lower = mean_val - min_val
        error_upper = max_val - mean_val

        # Plot mean as a single bar
        bar_width = 0.6
        mean_bar = ax.bar([0], [mean_val], bar_width,
                         color=color, alpha=0.6, edgecolor='black', linewidth=2,
                         label=f'Mean: {mean_val:.2f}')

        # Plot error bars (min/max range from mean)
        ax.errorbar([0], [mean_val],
                   yerr=[[error_lower], [error_upper]],
                   fmt='none', ecolor='darkred', capsize=15, capthick=3,
                   linewidth=3, label='Min/Max Range', zorder=5)

        # Plot individual run values as scatter points
        x_positions = np.zeros(len(values))
        # Add small jitter for visibility
        x_jitter = np.random.normal(0, 0.08, len(values))
        scatter = ax.scatter(x_positions + x_jitter, values,
                           color='darkblue', s=150, alpha=0.8, zorder=6,
                           edgecolors='black', linewidth=1.5,
                           label='Individual Runs')

        # Styling
        ax.set_ylabel(unit, fontsize=11, fontweight='bold')
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xticks([0])
        ax.set_xticklabels(['Average'], fontsize=11, fontweight='bold')
        ax.set_xlim(-0.8, 0.8)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.legend(loc='upper left', fontsize=9, framealpha=0.9)

        # Set y-axis limits: start from 0 with padding at top
        if metric in latency_metrics:
            # Use consistent max for all latency metrics
            y_max = max_latency * 1.15
        else:
            # For throughput, add 15% padding
            y_max = max_val * 1.15

        ax.set_ylim(0, y_max)

    fig.suptitle(f'Benchmark Results Summary ({len(results)} runs)',
                fontsize=14, fontweight='bold', y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    graph_path = output_dir / "benchmark_summary.png"
    fig.savefig(graph_path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Run memtier_benchmark and generate statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --runs 3 --time 30
  %(prog)s --runs 5 --time 60 --output ./results
  %(prog)s --no-graph --output /tmp/bench
        """,
    )
    parser.add_argument("-r", "--runs", type=int, default=3, help="Number of benchmark runs (default: 3)")
    parser.add_argument("-t", "--time", type=int, default=30, help="Test time in seconds (default: 30)")
    parser.add_argument("-T", "--threads", type=int, default=2, help="Number of threads (default: 2)")
    parser.add_argument("-c", "--clients", type=int, default=10, help="Clients per thread (default: 10)")
    parser.add_argument("--ratio", type=str, default="1:10", help="SET:GET ratio (default: 1:10)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Memcached host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=11211, help="Memcached port (default: 11211)")
    parser.add_argument("-o", "--output", type=str, default="./benchmark_results", help="Output directory (default: ./benchmark_results)")
    parser.add_argument("--name", type=str, default="", help="Benchmark name/label")
    parser.add_argument("--timeout", type=int, default=None, help="Timeout for each benchmark run in seconds (default: test_time * 2)")
    parser.add_argument("--no-graph", action="store_true", help="Skip graph generation")
    parser.add_argument("--analyze", type=str, default=None, help="Analyze existing results from CSV file (skips benchmark execution)")

    args = parser.parse_args()

    # Analyze mode: load existing CSV and generate reports
    if args.analyze:
        csv_path = Path(args.analyze)
        if not csv_path.exists():
            print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)

        # Load CSV data
        results = []
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append({
                    "total_ops": float(row["total_ops"]),
                    "set_ops": float(row["set_ops"]),
                    "get_ops": float(row["get_ops"]),
                    "avg_latency": float(row["avg_latency"]),
                    "p50_latency": float(row["p50_latency"]),
                    "p99_latency": float(row["p99_latency"]),
                    "p999_latency": float(row["p999_latency"]),
                    "kb_sec": float(row["kb_sec"]),
                    "timestamp": row.get("timestamp", ""),
                })

        if not results:
            print("Error: No data found in CSV file", file=sys.stderr)
            sys.exit(1)

        # Calculate statistics
        stats = calculate_stats(results)

        # Output directory (same as CSV directory or specified)
        output_dir = csv_path.parent
        if args.output != "./benchmark_results":
            output_dir = Path(args.output)
            output_dir.mkdir(parents=True, exist_ok=True)

        # Load or create config
        config_path = csv_path.parent / "config.json"
        if config_path.exists():
            with open(config_path, "r") as f:
                config = json.load(f)
        else:
            git_info = get_git_info()
            config = {
                "runs": len(results),
                "git": git_info,
                "analyzed_from": str(csv_path),
            }

        # Write summary JSON
        summary = {
            "config": config,
            "stats": stats,
            "runs": results,
        }
        summary_path = output_dir / "summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        # Generate graphs
        if not args.no_graph:
            generate_graphs(results, stats, output_dir)

        print(f"Analysis complete. Output saved to: {output_dir}")
        sys.exit(0)

    # Normal benchmark mode
    # Check memtier_benchmark availability
    try:
        subprocess.run(["memtier_benchmark", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: memtier_benchmark not found", file=sys.stderr)
        print("Install with: apt-get install memtier-benchmark", file=sys.stderr)
        sys.exit(1)

    # Create output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output) / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    # Configuration
    git_info = get_git_info()
    config = {
        "host": args.host,
        "port": args.port,
        "runs": args.runs,
        "test_time": args.time,
        "threads": args.threads,
        "clients": args.clients,
        "ratio": args.ratio,
        "name": args.name,
        "timestamp": timestamp,
        "git": git_info,
    }

    # Save config
    with open(output_dir / "config.json", "w") as f:
        json.dump(config, f, indent=2)

    # Calculate timeout
    timeout = args.timeout if args.timeout else args.time * 2

    print("=" * 60)
    print("BENCHMARK CONFIGURATION")
    print("=" * 60)
    print(f"Host:       {args.host}:{args.port}")
    print(f"Runs:       {args.runs}")
    print(f"Test Time:  {args.time}s")
    print(f"Timeout:    {timeout}s")
    print(f"Threads:    {args.threads}")
    print(f"Clients:    {args.clients} (per thread)")
    print(f"Ratio:      {args.ratio} (SET:GET)")
    print(f"Output:     {output_dir}")
    print(f"Git:        {git_info['branch']} ({git_info['commit']})")
    print("=" * 60)
    print()

    # Run benchmarks
    results = []
    for i in range(1, args.runs + 1):
        print(f"Run {i}/{args.runs}...", end=" ", flush=True)
        log_path = output_dir / f"run_{i}.log"
        try:
            result = run_benchmark(
                host=args.host,
                port=args.port,
                threads=args.threads,
                clients=args.clients,
                test_time=args.time,
                ratio=args.ratio,
                timeout=timeout,
                output_dir=output_dir,
                run_number=i,
            )
            result["timestamp"] = datetime.now().isoformat()
            results.append(result)
            print(f"done (total: {result['total_ops']:.2f} ops/sec, latency: {result['avg_latency']:.2f} ms)")
            print(f"  Log: {log_path}")
        except TimeoutError:
            print(f"TIMEOUT ({timeout}s exceeded)", file=sys.stderr)
            print(f"  Log: {log_path}", file=sys.stderr)
            continue
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip() if e.stderr else str(e)
            print(f"FAILED: {error_msg.split(chr(10))[0][:60]}", file=sys.stderr)
            print(f"  Log: {log_path}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"ERROR: {str(e)[:60]}", file=sys.stderr)
            print(f"  Log: {log_path}", file=sys.stderr)
            continue

        if i < args.runs:
            print("Waiting 5 seconds before next run...")
            import time
            time.sleep(5)

    if not results:
        print("Error: No successful benchmark runs", file=sys.stderr)
        sys.exit(1)

    # Calculate statistics
    stats = calculate_stats(results)

    # Write CSV
    csv_path = output_dir / "results.csv"
    write_csv(results, csv_path)

    # Write summary JSON
    summary = {
        "config": config,
        "stats": stats,
        "runs": results,
    }
    summary_path = output_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    # Generate graphs
    if not args.no_graph:
        generate_graphs(results, stats, output_dir)

    print(f"Benchmark complete. Output saved to: {output_dir}")


if __name__ == "__main__":
    main()
