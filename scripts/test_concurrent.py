#!/usr/bin/env python3
"""Concurrent stress test for duckdb-rpc.

Starts an RPC server, spawns concurrent reader and writer workers,
and reports latency/throughput stats.

Usage:
    uv run --with duckdb==1.5.1 python scripts/test_concurrent.py
    uv run --with duckdb==1.5.1 python scripts/test_concurrent.py --address http://localhost:19444
    uv run --with duckdb==1.5.1 python scripts/test_concurrent.py --readers 8 --writers 4 --iterations 100
"""

import argparse
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
EXTENSION_PATH = os.path.join(PROJECT_DIR, "build", "release", "extension", "rpc", "rpc.duckdb_extension")


def make_connection():
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{EXTENSION_PATH}'")
    return conn


def setup_server(address):
    conn = make_connection()
    conn.execute("CREATE TABLE test_data AS SELECT i, 'row_' || i AS name FROM range(10000) t(i)")
    conn.execute(f"CALL quack_serve('{address}')")
    time.sleep(0.2)
    return conn


def reader_worker(address, iterations, worker_id):
    conn = make_connection()
    conn.execute(f"ATTACH '{address}' AS remote_{worker_id} (TYPE rpc)")

    latencies = []
    errors = 0

    for _ in range(iterations):
        threshold = random.randint(0, 9999)
        try:
            start = time.perf_counter()
            conn.execute(
                f"SELECT count(*), max(i) FROM remote_{worker_id}.main.test_data WHERE i > {threshold}"
            ).fetchall()
            latencies.append(time.perf_counter() - start)
        except Exception as e:
            errors += 1
            print(f"  [reader-{worker_id}] error: {e}")

    conn.close()
    return {"type": "reader", "id": worker_id, "latencies": latencies, "errors": errors}


def writer_worker(address, iterations, worker_id, id_offset):
    conn = make_connection()
    conn.execute(f"ATTACH '{address}' AS remote_{worker_id} (TYPE rpc, READ_WRITE)")

    latencies = []
    errors = 0

    for i in range(iterations):
        row_id = id_offset + i
        try:
            start = time.perf_counter()
            conn.execute(
                f"INSERT INTO remote_{worker_id}.main.test_data VALUES ({row_id}, 'written_{row_id}')"
            )
            latencies.append(time.perf_counter() - start)
        except Exception as e:
            errors += 1
            print(f"  [writer-{worker_id}] error: {e}")

    conn.close()
    return {"type": "writer", "id": worker_id, "latencies": latencies, "errors": errors}


def percentile(data, p):
    if not data:
        return 0
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[-1]
    return data[f] + (k - f) * (data[c] - data[f])


def print_stats(label, latencies):
    if not latencies:
        print(f"  {label}: no successful ops")
        return
    latencies.sort()
    total = len(latencies)
    total_time = sum(latencies)
    print(f"  {label}:")
    print(f"    ops: {total}, total time: {total_time:.2f}s, throughput: {total / total_time:.1f} ops/s")
    print(f"    avg: {statistics.mean(latencies) * 1000:.1f}ms")
    print(f"    p50: {percentile(latencies, 50) * 1000:.1f}ms")
    print(f"    p95: {percentile(latencies, 95) * 1000:.1f}ms")
    print(f"    p99: {percentile(latencies, 99) * 1000:.1f}ms")


def main():
    parser = argparse.ArgumentParser(description="Concurrent stress test for duckdb-rpc")
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Listen address (e.g. http://localhost:19444 or /tmp/rpc.sock). "
             "Defaults to a unix socket at /tmp/duckdb-rpc-test-<pid>",
    )
    parser.add_argument("--readers", type=int, default=4, help="Number of reader workers")
    parser.add_argument("--writers", type=int, default=2, help="Number of writer workers")
    parser.add_argument("--iterations", type=int, default=50, help="Iterations per worker")
    args = parser.parse_args()

    is_unix_socket = False
    if args.address:
        address = args.address
    else:
        address = f"/tmp/duckdb-rpc-test-{os.getpid()}"
        is_unix_socket = True

    print(f"Starting server on {address}")
    server_conn = setup_server(address)

    print(f"Launching {args.readers} readers + {args.writers} writers, {args.iterations} iterations each")
    wall_start = time.perf_counter()

    futures = []
    with ThreadPoolExecutor(max_workers=args.readers + args.writers) as pool:
        for i in range(args.readers):
            futures.append(pool.submit(reader_worker, address, args.iterations, i))
        for i in range(args.writers):
            id_offset = 100_000 + i * args.iterations
            futures.append(pool.submit(writer_worker, address, args.iterations, i, id_offset))

        results = [f.result() for f in as_completed(futures)]

    wall_time = time.perf_counter() - wall_start

    read_latencies = []
    write_latencies = []
    total_errors = 0
    for r in results:
        total_errors += r["errors"]
        if r["type"] == "reader":
            read_latencies.extend(r["latencies"])
        else:
            write_latencies.extend(r["latencies"])

    print(f"\n--- Results (wall time: {wall_time:.2f}s) ---")
    print_stats("Reads", read_latencies)
    print_stats("Writes", write_latencies)
    total_ops = len(read_latencies) + len(write_latencies)
    print(f"  Total: {total_ops} ops, {total_ops / wall_time:.1f} ops/s, {total_errors} errors")

    print("\nTearing down...")
    server_conn.execute(f"CALL quack_stop('{address}')")
    server_conn.close()
    if is_unix_socket:
        try:
            os.unlink(address)
        except FileNotFoundError:
            pass

    print("Done.")


if __name__ == "__main__":
    main()
