"""Benchmark writes to Lance tables: vanilla Spark, Gluten with lance-spark's row writer, and Gluten
with the native Velox writer.

Run each setup in its own spark-submit (see run_local.sh). Two commands:

  gen    writes the input Parquet once:
         lance_write_bench.py gen --dataset lineitem --rows 60000000 --files 48 --out /data/in
         lance_write_bench.py gen --dataset vectors --rows 1000000 --dim 768 --out /data/vec

  write  times writes of that input to new Lance tables and appends one CSV row per write:
         lance_write_bench.py write --input /data/in --target /data/out --label native \
             --reps 3 --results results.csv [--vector-col v --dim 768]

The label is free text; run_local.sh uses vanilla, gluten-row and gluten-native.
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import time
import urllib.request

from pyspark.sql import SparkSession

FSL_KEY = "arrow.fixed-size-list.size"


def gen(spark, args):
    df = spark.range(0, args.rows, 1, args.files)
    if args.dataset == "lineitem":
        # TPC-H lineitem's columns and types, with values derived from the row id.
        df = df.selectExpr(
            "id as l_orderkey",
            "id % 200000 as l_partkey",
            "id % 10000 as l_suppkey",
            "cast(id % 7 + 1 as int) as l_linenumber",
            "cast(id % 50 + 1 as decimal(12, 2)) as l_quantity",
            "cast(id % 100000 / 100.0 + 900 as decimal(12, 2)) as l_extendedprice",
            "cast(id % 11 / 100.0 as decimal(12, 2)) as l_discount",
            "cast(id % 9 / 100.0 as decimal(12, 2)) as l_tax",
            "substr('ARN', cast(id % 3 as int) + 1, 1) as l_returnflag",
            "substr('FO', cast(id % 2 as int) + 1, 1) as l_linestatus",
            "date_add(date'1992-01-01', cast(id % 2500 as int)) as l_shipdate",
            "date_add(date'1992-01-31', cast(id % 2500 as int)) as l_commitdate",
            "date_add(date'1992-01-15', cast(id % 2500 as int)) as l_receiptdate",
            "element_at(array('DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'),"
            " cast(id % 4 as int) + 1) as l_shipinstruct",
            "element_at(array('TRUCK', 'MAIL', 'REG AIR', 'AIR', 'FOB', 'RAIL', 'SHIP'),"
            " cast(id % 7 as int) + 1) as l_shipmode",
            "substr(sha2(cast(id as string), 256), 1, 10 + cast(id % 34 as int)) as l_comment",
        )
    elif args.dataset == "vectors":
        # Pseudo-random floats in [-1, 1), so the vectors do not compress unrealistically well.
        v = (
            f"transform(sequence(0, {args.dim - 1}),"
            f" j -> cast(hash(id, j) / 2147483648.0 as float))"
        )
        if args.null_every > 0:
            v = f"if(id % {args.null_every} = 0, null, {v})"
        df = df.selectExpr("id", f"{v} as v")
    else:
        raise ValueError(args.dataset)
    df.write.mode("overwrite").parquet(args.out)


def read_input(spark, args):
    df = spark.read.parquet(args.input)
    if args.vector_col:
        # lance-spark writes a fixed-size list for an array column with this metadata.
        meta = {FSL_KEY: args.dim}
        df = df.withColumn(args.vector_col, df[args.vector_col].alias(args.vector_col, metadata=meta))
    return df


def rest(spark, path):
    url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}{path}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def group_metrics(spark, group):
    """Sums the stage metrics of the jobs in a job group. The REST API lags the listener bus a bit,
    so it retries until every stage of those jobs has completed."""
    for _ in range(50):
        jobs = [j for j in rest(spark, "/jobs") if j.get("jobGroup") == group]
        stage_ids = {s for j in jobs for s in j["stageIds"]}
        stages = [s for s in rest(spark, "/stages") if s["stageId"] in stage_ids]
        if jobs and stages and all(s["status"] in ("COMPLETE", "SKIPPED") for s in stages):
            done = [s for s in stages if s["status"] == "COMPLETE"]
            return {
                "jobs": len(jobs),
                "tasks": sum(s["numCompleteTasks"] for s in done),
                "cpu_s": sum(s["executorCpuTime"] for s in done) / 1e9,
                "run_s": sum(s["executorRunTime"] for s in done) / 1e3,
                "gc_s": sum(s["jvmGcTime"] for s in done) / 1e3,
                "input_mb": sum(s["inputBytes"] for s in done) / 2**20,
            }
        time.sleep(0.2)
    raise RuntimeError(f"metrics for job group {group} did not complete")


def du_mb(path):
    if "://" in path and not path.startswith("file://"):
        return float("nan")
    out = subprocess.run(["du", "-sk", path.replace("file://", "")], capture_output=True, text=True)
    return int(out.stdout.split()[0]) / 1024 if out.returncode == 0 else float("nan")


def remove(path):
    if "://" not in path or path.startswith("file://"):
        shutil.rmtree(path.replace("file://", ""), ignore_errors=True)


def process_cpu_s(pid):
    """CPU time of the JVM process, all threads included: Spark's executorCpuTime only counts task
    threads, while Lance encodes and writes on its own threads. Only meaningful in local mode."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except OSError:
        # macOS: [[dd-]hh:]mm:ss.cc
        out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True)
        days, _, clock = out.stdout.strip().rpartition("-")
        total = 0.0
        for part in clock.split(":"):
            total = total * 60 + float(part)
        return total + (int(days) * 86400 if days else 0)


def timed(spark, group, action):
    local = spark.sparkContext.master.startswith("local")
    pid = spark.sparkContext._jvm.java.lang.ProcessHandle.current().pid() if local else None
    spark.sparkContext.setJobGroup(group, group)
    cpu0 = process_cpu_s(pid) if local else 0.0
    t0 = time.perf_counter()
    action()
    wall = time.perf_counter() - t0
    cpu1 = process_cpu_s(pid) if local else float("nan")
    spark.sparkContext.setJobGroup("", "")
    metrics = group_metrics(spark, group)
    metrics["process_cpu_s"] = cpu1 - cpu0
    return wall, metrics


def checksum(spark, df):
    row = df.selectExpr("count(*) as n", "sum(xxhash64(*)) as h").collect()[0]
    return f"{row['n']}:{row['h']}"


def write(spark, args):
    df = read_input(spark, args)
    rows = df.count()
    expected = checksum(spark, df)
    conf = spark.sparkContext.getConf()
    setup = {
        "gluten": "org.apache.gluten.GlutenPlugin" in conf.get("spark.plugins", ""),
        "native": conf.get("spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite", "false"),
        "master": spark.sparkContext.master,
        "cores": spark.sparkContext.defaultParallelism,
    }
    print(f"BENCH setup {args.label}: {setup}, input {rows} rows", flush=True)

    results = []
    for i in range(args.warmup + args.reps):
        warm = i < args.warmup
        kind = "warmup" if warm else "rep"
        # Scan only, through Spark's no-op sink.
        wall, m = timed(
            spark, f"{args.label}-scan-{i}",
            lambda: read_input(spark, args).write.format("noop").mode("overwrite").save())
        if not warm:
            results.append(("scan", i, wall, m, float("nan")))
        # A new table each time, so every write does the same work.
        uri = os.path.join(args.target, f"{args.label}-{i}.lance")
        remove(uri)
        wall, m = timed(
            spark, f"{args.label}-write-{i}",
            lambda: read_input(spark, args).write.format("lance").option("path", uri).save())
        size = du_mb(uri)
        if not warm:
            results.append(("write", i, wall, m, size))
        print(f"BENCH {args.label} {kind} {i}: write {wall:.2f} s, process cpu "
              f"{m['process_cpu_s']:.1f} s, task cpu {m['cpu_s']:.1f} s, {size:.0f} MB", flush=True)
        if i < args.warmup + args.reps - 1:
            remove(uri)

    # Read the last table back with lance-spark's reader, as the Gluten Lance scan does not load
    # fixed-size list columns yet.
    spark.conf.set("spark.gluten.sql.columnar.batchscan", "false")
    actual = checksum(spark, spark.read.format("lance").option("path", uri).load())
    remove(uri)
    if actual != expected:
        raise RuntimeError(f"{args.label}: read back {actual}, expected {expected}")
    print(f"BENCH {args.label}: read back matches the input ({actual})", flush=True)

    new_file = not os.path.exists(args.results)
    with open(args.results, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["dataset", "label", "measure", "rep", "rows", "wall_s", "process_cpu_s",
                        "cpu_s", "run_s", "gc_s", "tasks", "jobs", "input_mb", "output_mb", "cores",
                        "checksum"])
        for measure, i, wall, m, size in results:
            w.writerow([args.dataset_name, args.label, measure, i, rows, f"{wall:.3f}",
                        f"{m['process_cpu_s']:.2f}", f"{m['cpu_s']:.2f}", f"{m['run_s']:.2f}", f"{m['gc_s']:.2f}", m["tasks"],
                        m["jobs"], f"{m['input_mb']:.1f}", f"{size:.1f}", setup["cores"], actual])


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gen")
    g.add_argument("--dataset", choices=["lineitem", "vectors"], required=True)
    g.add_argument("--rows", type=int, required=True)
    g.add_argument("--files", type=int, default=48)
    g.add_argument("--dim", type=int, default=768)
    g.add_argument("--null-every", type=int, default=0)
    g.add_argument("--out", required=True)
    w = sub.add_parser("write")
    w.add_argument("--input", required=True)
    w.add_argument("--target", required=True)
    w.add_argument("--label", required=True)
    w.add_argument("--dataset-name", default="")
    w.add_argument("--reps", type=int, default=3)
    w.add_argument("--warmup", type=int, default=1)
    w.add_argument("--results", default="results.csv")
    w.add_argument("--vector-col", default="")
    w.add_argument("--dim", type=int, default=768)
    args = p.parse_args()

    spark = SparkSession.builder.appName(f"lance-write-bench-{args.cmd}").getOrCreate()
    try:
        if args.cmd == "gen":
            gen(spark, args)
        else:
            args.dataset_name = args.dataset_name or os.path.basename(args.input.rstrip("/"))
            write(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
