"""SPAR-style order pipeline: validate -> translate -> process.

Each function is triggered by a new blob (through Event Grid), does one
small job, and writes its result to the next container:

    inbound -> to-translate -> to-process -> to-deliver

The order file's base name (e.g. "order-0001") is the correlation ID.
Every function logs it, so one Application Insights query can follow
a single order through all three stages.

Timing: each function records when it started, in UTC with millisecond
precision, and the times travel with the order. The final JSON in
to-deliver carries a "timings" block:
    validateStart, translateStart, processStart, done
The extra HTTP function "now" returns the server's current time, so the
load test can line up its own clock with Azure's.
"""
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import azure.functions as func

app = func.FunctionApp()

STORAGE = "AzureWebJobsStorage"  # app setting holding the storage connection
REQUIRED_FIELDS = ("OrderId", "StoreId", "OrderDate", "Lines")


# ---------- pure logic (no Azure code, easy to unit test) ----------

def now_iso() -> str:
    """Current UTC time with milliseconds, e.g. 2026-11-19T05:10:12.345+00:00"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def correlation_id(blob_name: str) -> str:
    """'inbound/order-0001.xml' -> 'order-0001'"""
    return blob_name.split("/")[-1].split(".")[0]


def validate_order(xml_text: str) -> list[str]:
    """Return a list of problems; an empty list means the order is valid."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        return [f"not well-formed XML: {exc}"]
    problems = [f"missing <{f}>" for f in REQUIRED_FIELDS if root.find(f) is None]
    lines = root.find("Lines")
    if lines is not None and len(lines.findall("Line")) == 0:
        problems.append("order has no <Line> items")
    return problems


def stamp_validated(xml_text: str, validate_start: str) -> str:
    """Return the order XML with the validate start time added as an attribute."""
    root = ET.fromstring(xml_text)
    root.set("validateStart", validate_start)
    return ET.tostring(root, encoding="unicode")


def translate_order(xml_text: str, translate_start: str | None = None) -> dict:
    """Convert the order XML into a plain dictionary (later saved as JSON)."""
    root = ET.fromstring(xml_text)
    order = {
        "orderId": root.findtext("OrderId"),
        "storeId": root.findtext("StoreId"),
        "orderDate": root.findtext("OrderDate"),
        "lines": [
            {
                "sku": line.findtext("Sku"),
                "quantity": int(line.findtext("Quantity")),
                "unitPrice": float(line.findtext("UnitPrice")),
            }
            for line in root.find("Lines").findall("Line")
        ],
        "timings": {},
    }
    if root.get("validateStart"):
        order["timings"]["validateStart"] = root.get("validateStart")
    if translate_start:
        order["timings"]["translateStart"] = translate_start
    return order


def process_order(order: dict, process_start: str | None = None) -> dict:
    """Enrich the order with totals, and record when processing started and ended."""
    order["lineCount"] = len(order["lines"])
    order["orderTotal"] = round(
        sum(l["quantity"] * l["unitPrice"] for l in order["lines"]), 2
    )
    timings = order.setdefault("timings", {})
    if process_start:
        timings["processStart"] = process_start
    timings["done"] = now_iso()
    return order


def log_stage(stage: str, cid: str, status: str) -> None:
    # One consistent line per stage; KQL queries parse it.
    logging.info("PIPELINE stage=%s correlationId=%s status=%s", stage, cid, status)


# ---------- the three pipeline functions ----------

@app.blob_trigger(arg_name="blob", path="inbound/{name}",
                  connection=STORAGE, source="EventGrid")
@app.blob_output(arg_name="out", path="to-translate/{name}", connection=STORAGE)
def validate(blob: func.InputStream, out: func.Out[str]) -> None:
    started = now_iso()
    cid = correlation_id(blob.name)
    text = blob.read().decode("utf-8")
    problems = validate_order(text)
    if problems:
        logging.error("PIPELINE stage=validate correlationId=%s status=rejected "
                      "problems=%s", cid, problems)
        return  # nothing is written, so the pipeline stops here for this file
    out.set(stamp_validated(text, started))
    log_stage("validate", cid, "ok")


@app.blob_trigger(arg_name="blob", path="to-translate/{name}",
                  connection=STORAGE, source="EventGrid")
@app.blob_output(arg_name="out", path="to-process/{name}.json", connection=STORAGE)
def translate(blob: func.InputStream, out: func.Out[str]) -> None:
    started = now_iso()
    cid = correlation_id(blob.name)
    order = translate_order(blob.read().decode("utf-8"), started)
    out.set(json.dumps(order))
    log_stage("translate", cid, "ok")


@app.blob_trigger(arg_name="blob", path="to-process/{name}",
                  connection=STORAGE, source="EventGrid")
@app.blob_output(arg_name="out", path="to-deliver/{name}", connection=STORAGE)
def process(blob: func.InputStream, out: func.Out[str]) -> None:
    started = now_iso()
    cid = correlation_id(blob.name)
    order = process_order(json.loads(blob.read().decode("utf-8")), started)
    out.set(json.dumps(order, indent=2))
    log_stage("process", cid, "ok")


# ---------- clock check for the load test ----------

@app.route(route="now", auth_level=func.AuthLevel.ANONYMOUS)
def now(req: func.HttpRequest) -> func.HttpResponse:
    """Return the server's current UTC time. Used only to align clocks."""
    return func.HttpResponse(json.dumps({"utc": now_iso()}),
                             mimetype="application/json")
