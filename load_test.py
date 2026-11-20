"""Load test for the SPAR demo pipeline (millisecond timing).

Generates synthetic order files, uploads them to the 'inbound' container,
waits for the results in 'to-deliver', and reports end-to-end latency.

How latency is measured, per file:
  start = the moment the upload of the order finished (this PC's clock,
          corrected to Azure's clock, see below)
  end   = the "done" time the 'process' function wrote into the result
          (Azure's clock)
  latency = end - start, in milliseconds, cold starts included.

Clock alignment: after the run, the script asks the Function App's "now"
endpoint for the server time a few times and works out how far this PC's
clock is from Azure's. The uncertainty is printed (usually tens of ms).

The result JSON also shows when each function started, so the script
reports where the time went: the wait before 'validate' starts (Event Grid
delivery plus any cold start) and each stage after that.

Setup (once per terminal window, PowerShell):
    py -3.12 -m pip install azure-storage-blob
    $env:SPAR_STORAGE_CONNECTION="<storage account connection string>"
    $env:SPAR_APP_URL="https://<function app default domain>"

Examples:
    py -3.12 load_test.py --run-id flexA1 --pattern steady --count 150 --rate 5
    py -3.12 load_test.py --run-id flexB1 --pattern burst  --count 1000
    py -3.12 load_test.py --run-id flexC1 --pattern cold   --count 100
    py -3.12 load_test.py --run-id flexR1 --pattern random --count 10000 --rate 170
"""
import argparse
import csv
import json
import math
import os
import random
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

SKUS = [("MILK-1L", 1.19), ("BREAD-WW", 2.49), ("COFFEE-250G", 5.99),
        ("EGGS-12", 3.29), ("BUTTER-250G", 2.79), ("APPLE-1KG", 2.99),
        ("RICE-1KG", 1.89), ("PASTA-500G", 0.99), ("CHEESE-400G", 4.49),
        ("JUICE-1L", 1.79)]

STAGES = [  # (label, from timing, to timing)
    ("wait before validate", "uploaded", "validateStart"),
    ("validate -> translate", "validateStart", "translateStart"),
    ("translate -> process", "translateStart", "processStart"),
    ("process",              "processStart", "done"),
]


# ---------- pure logic (testable without Azure) ----------

def make_order_xml(order_number: int, rng: random.Random) -> str:
    """One synthetic order with 3-10 lines, fake store and products."""
    lines = "".join(
        f"<Line><Sku>{sku}</Sku><Quantity>{rng.randint(1, 48)}</Quantity>"
        f"<UnitPrice>{price}</UnitPrice></Line>"
        for sku, price in rng.sample(SKUS, rng.randint(3, 10))
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f"<Order><OrderId>ORD-{order_number:06d}</OrderId>"
        f"<StoreId>STORE-{rng.randint(1, 450):03d}</StoreId>"
        f"<OrderDate>{time.strftime('%Y-%m-%d')}</OrderDate>"
        f"<Lines>{lines}</Lines></Order>"
    )


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile: the value that pct% of results are at or below."""
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def to_epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def clock_offset(app_url: str, tries: int = 5) -> tuple[float, float]:
    """(Azure time minus this PC's time, uncertainty), both in seconds."""
    url = app_url.rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    best = None
    for _ in range(tries):
        t0 = time.time()
        with urllib.request.urlopen(url + "/api/now", timeout=60) as resp:
            server = to_epoch(json.load(resp)["utc"])
        t1 = time.time()
        offset, uncertainty = server - (t0 + t1) / 2, (t1 - t0) / 2
        if best is None or uncertainty < best[1]:
            best = (offset, uncertainty)
    return best


# ---------- Azure part ----------

def delivered_names(container, prefix: str) -> dict:
    """{'flexA1-000001': 'flexA1-000001.xml.json', ...} for this run."""
    return {b.name.split(".")[0]: b.name
            for b in container.list_blobs(name_starts_with=prefix)}


def main() -> None:
    p = argparse.ArgumentParser(description="Load test for the SPAR demo pipeline")
    p.add_argument("--run-id", required=True,
                   help="unique label for this run, letters/digits only, e.g. flexA1")
    p.add_argument("--pattern", required=True,
                   choices=["steady", "burst", "cold", "random"])
    p.add_argument("--count", type=int, required=True, help="number of files")
    p.add_argument("--rate", type=float, default=5,
                   help="files per minute (steady: exact; random: average)")
    p.add_argument("--timeout", type=int, default=600,
                   help="seconds to wait for results after the last upload")
    args = p.parse_args()

    if not args.run_id.isalnum():
        sys.exit("--run-id must contain only letters and digits")
    conn = os.environ.get("SPAR_STORAGE_CONNECTION")
    if not conn:
        sys.exit("Set the SPAR_STORAGE_CONNECTION environment variable first")
    app_url = os.environ.get("SPAR_APP_URL")
    if not app_url:
        sys.exit("Set the SPAR_APP_URL environment variable first "
                 "(the Function App's default domain)")

    from azure.storage.blob import BlobServiceClient
    service = BlobServiceClient.from_connection_string(conn)
    inbound = service.get_container_client("inbound")
    deliver = service.get_container_client("to-deliver")

    prefix = f"{args.run_id}-"
    if any(True for _ in inbound.list_blobs(name_starts_with=prefix)):
        sys.exit(f"Run id '{args.run_id}' was already used. Pick a new one.")

    rng = random.Random(args.run_id)  # same run id -> same data, for fairness
    orders = [(f"{prefix}{i:06d}.xml", make_order_xml(i, rng))
              for i in range(1, args.count + 1)]
    uploaded = {}  # correlation id -> this PC's time when its upload finished

    def upload(item) -> None:
        name, xml = item
        inbound.upload_blob(name, xml)
        uploaded[name.split(".")[0]] = time.time()

    print(f"Run {args.run_id}: pattern={args.pattern}, files={args.count}")
    started = time.time()
    if args.pattern == "steady":
        gap = 60.0 / args.rate
        for n, item in enumerate(orders, 1):
            upload(item)
            print(f"\r  uploaded {n}/{args.count}", end="", flush=True)
            if n < args.count:
                time.sleep(gap)
    elif args.pattern == "random":
        # Random arrivals: exponential gaps, so files come irregularly at the
        # given average rate, like orders arriving from many stores.
        gaps = [rng.expovariate(args.rate / 60.0) for _ in orders]
        print(f"  expected duration: about {sum(gaps) / 60:.0f} minutes")
        t0, offset, futures = time.time(), 0.0, []
        with ThreadPoolExecutor(max_workers=32) as pool:
            for n, (item, gap) in enumerate(zip(orders, gaps), 1):
                offset += gap
                delay = t0 + offset - time.time()
                if delay > 0:
                    time.sleep(delay)
                futures.append(pool.submit(upload, item))
                if n % 100 == 0 or n == args.count:
                    print(f"\r  uploaded {n}/{args.count}", end="", flush=True)
            for f in futures:
                f.result()  # surface any upload error
    else:
        if args.pattern == "cold":
            # First file alone: it pays the cold start. Wait for it to finish.
            upload(orders[0])
            print("  first file uploaded, waiting for it to be delivered...")
            first_cid = orders[0][0].split(".")[0]
            deadline = time.time() + args.timeout
            while first_cid not in delivered_names(deliver, prefix):
                if time.time() > deadline:
                    sys.exit("First file was not delivered before the timeout.")
                time.sleep(1)
            rest = orders[1:]
        else:
            rest = orders
        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(upload, rest))
        print(f"  uploaded {args.count}/{args.count}", end="")
    print(f"\n  upload phase took {time.time() - started:.0f} s")

    # Wait for results.
    deadline = time.time() + args.timeout
    while True:
        done = delivered_names(deliver, prefix)
        print(f"\r  delivered {len(done)}/{args.count}", end="", flush=True)
        if len(done) >= args.count or time.time() > deadline:
            break
        time.sleep(5)
    print()

    # Read the timings each result carries.
    def read_timings(cid):
        data = json.loads(deliver.download_blob(done[cid]).readall())
        return cid, data.get("timings", {})
    print("  reading timings from results...")
    with ThreadPoolExecutor(max_workers=32) as pool:
        timings = dict(pool.map(read_timings, list(done)))

    # Line up this PC's clock with Azure's (done after the run, so it
    # cannot warm the app before the test).
    offset, uncertainty = clock_offset(app_url)

    rows, latencies, no_timings = [], [], 0
    stage_values = {label: [] for label, _, _ in STAGES}
    for cid in sorted(uploaded):
        t = {"uploaded": uploaded[cid] + offset}
        for key, value in timings.get(cid, {}).items():
            t[key] = to_epoch(value)
        if cid in done and "done" not in t:
            no_timings += 1
        latency = t["done"] - t["uploaded"] if "done" in t else None
        if latency is not None:
            latencies.append(latency)
            for label, a, b in STAGES:
                if a in t and b in t:
                    stage_values[label].append(t[b] - t[a])
        rows.append([cid] + [
            datetime.fromtimestamp(t[k]).astimezone().isoformat(timespec="milliseconds")
            if k in t else ""
            for k in ("uploaded", "validateStart", "translateStart", "processStart", "done")
        ] + [f"{latency * 1000:.0f}" if latency is not None else ""])

    out = f"results-{args.run_id}.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["correlation_id", "uploaded", "validate_start", "translate_start",
                    "process_start", "done", "latency_ms"])
        w.writerows(rows)

    def ms(seconds):
        return f"{seconds * 1000:,.0f} ms"

    print(f"\nResults for run {args.run_id}")
    print(f"  files sent:      {len(uploaded)}")
    print(f"  files delivered: {len(done)}  (missing: {len(uploaded) - len(done)})")
    if no_timings:
        print(f"  WARNING: {no_timings} results had no timings "
              "(was the new function code deployed?)")
    if latencies:
        print(f"  median latency:  {ms(percentile(latencies, 50))}")
        print(f"  p95 latency:     {ms(percentile(latencies, 95))}")
        print(f"  slowest file:    {ms(max(latencies))}")
        first = orders[0][0].split(".")[0]
        if args.pattern == "cold" and first in timings and "done" in timings[first]:
            print(f"  first file (cold start): "
                  f"{ms(to_epoch(timings[first]['done']) - uploaded[first] - offset)}")
        span = (max(to_epoch(v["done"]) for v in timings.values() if "done" in v)
                - (min(uploaded.values()) + offset))
        if span > 0:
            print(f"  throughput:      {len(latencies) / span * 60:.0f} files/minute")
        print("  where the time went (median, p95):")
        for label, values in stage_values.items():
            if values:
                print(f"    {label:<22} {ms(percentile(values, 50)):>10}"
                      f"  {ms(percentile(values, 95)):>10}")
    print(f"  clock alignment: PC is {abs(offset) * 1000:.0f} ms "
          f"{'behind' if offset > 0 else 'ahead of'} Azure "
          f"(accurate to about +/- {uncertainty * 1000:.0f} ms)")
    print(f"  details saved to {out}")


if __name__ == "__main__":
    main()
