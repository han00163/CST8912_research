"""Local test of the pipeline logic. Run:  py -3.12 test_logic.py
Needs no Azure account. It stubs the Azure library if it isn't installed."""
import sys
import types
from datetime import datetime

try:
    import azure.functions  # noqa: F401
except ImportError:
    class _App:
        def _deco(self, *a, **k):
            return lambda f: f
        blob_trigger = blob_output = route = _deco
    stub = types.ModuleType("azure.functions")
    stub.FunctionApp = _App
    stub.InputStream = stub.HttpRequest = stub.HttpResponse = object
    stub.AuthLevel = types.SimpleNamespace(ANONYMOUS="anonymous")
    class _Out:
        def __class_getitem__(cls, item): return cls
    stub.Out = _Out
    sys.modules["azure"] = types.ModuleType("azure")
    sys.modules["azure.functions"] = stub

import function_app as fa

xml = open("order-0001.xml", encoding="utf-8").read()

# correlation IDs
assert fa.correlation_id("inbound/order-0001.xml") == "order-0001"
assert fa.correlation_id("to-deliver/order-0001.xml.json") == "order-0001"

# validation
assert fa.validate_order(xml) == []
assert fa.validate_order("<Order><OrderId>1</OrderId></Order>") != []
assert fa.validate_order("not xml")[0].startswith("not well-formed")

# the full chain, with timings carried through
t1 = fa.now_iso()
stamped = fa.stamp_validated(xml, t1)
assert fa.validate_order(stamped) == []          # still a valid order
t2 = fa.now_iso()
order = fa.translate_order(stamped, t2)
t3 = fa.now_iso()
order = fa.process_order(order, t3)
timings = order["timings"]
assert list(timings) == ["validateStart", "translateStart", "processStart", "done"]
times = [datetime.fromisoformat(timings[k]) for k in timings]
assert times == sorted(times), "timings must be in order"
assert order["lineCount"] == 3
assert order["orderTotal"] == round(24*1.19 + 12*2.49 + 6*5.99, 2)

# an old-style order without timings still works
assert fa.process_order(fa.translate_order(xml))["orderTotal"] == order["orderTotal"]

print("All checks passed. Order total:", order["orderTotal"])
print("Timings:", timings)
