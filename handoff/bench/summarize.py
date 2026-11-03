"""Prints a Markdown summary of results.csv from lance_write_bench.py: medians per dataset and setup,
and speedups against vanilla Spark and against Gluten's row writer."""

import csv
import statistics
import sys
from collections import defaultdict


def main(path):
    runs = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            runs[(r["dataset"], r["label"], r["measure"])].append(r)

    datasets = sorted({d for d, _, _ in runs})
    for dataset in datasets:
        writes = {label: rs for (d, label, m), rs in runs.items() if d == dataset and m == "write"}
        scans = {label: rs for (d, label, m), rs in runs.items() if d == dataset and m == "scan"}
        if not writes:
            continue
        rows = int(next(iter(writes.values()))[0]["rows"])
        cores = next(iter(writes.values()))[0]["cores"]
        med = {label: statistics.median(float(r["wall_s"]) for r in rs) for label, rs in writes.items()}
        print(f"\n### {dataset}: {rows:,} rows, {cores} cores\n")
        print("| Setup | Write (median) | Range | Rows/s | Process CPU | Task CPU | GC | Scan only |"
              " Output | vs vanilla | vs Gluten row writer |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")
        for label in ["vanilla", "gluten-row", "gluten-native"] + sorted(set(writes) - {
                "vanilla", "gluten-row", "gluten-native"}):
            if label not in writes:
                continue
            rs = writes[label]
            walls = [float(r["wall_s"]) for r in rs]
            cpu = statistics.median(float(r["cpu_s"]) for r in rs)
            pcpu = statistics.median(float(r["process_cpu_s"]) for r in rs)
            gc = statistics.median(float(r["gc_s"]) for r in rs)
            out = statistics.median(float(r["output_mb"]) for r in rs)
            scan = (f"{statistics.median(float(r['wall_s']) for r in scans[label]):.2f} s"
                    if label in scans else "")
            vs_vanilla = f"{med['vanilla'] / med[label]:.2f}x" if "vanilla" in med else ""
            vs_row = f"{med['gluten-row'] / med[label]:.2f}x" if "gluten-row" in med else ""
            print(f"| {label} | {med[label]:.2f} s | {min(walls):.2f}-{max(walls):.2f} s |"
                  f" {rows / med[label]:,.0f} | {pcpu:.1f} s | {cpu:.1f} s | {gc:.1f} s | {scan} | {out:,.0f} MB |"
                  f" {vs_vanilla} | {vs_row} |")
        sums = {r["checksum"] for rs in writes.values() for r in rs}
        print(f"\nRead-back checksums: {'all equal' if len(sums) == 1 else 'DIFFER: ' + str(sums)}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results.csv")
