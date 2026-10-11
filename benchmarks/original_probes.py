"""Execute selected unmodified original functions without starting its infinite runtime."""

import ast
import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

original = Path(sys.argv[1]).resolve()
source = original / "src/framework/etl/framework_etl.py"
tree = ast.parse(source.read_text())
names = {"CommitCoordinator", "_handle_retry_to_dlq", "_forward_result"}
nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
module = ast.Module(
    body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        *nodes,
    ],
    type_ignores=[],
)
calls = []
namespace = {
    "threading": threading,
    "time": time,
    "OffsetAndMetadata": object,
    "_commit_offsets": lambda consumer, offsets: calls.append(dict(offsets)),
    "_send_sync": lambda *a, **k: False,
    "logger": SimpleNamespace(debug=lambda *a: None),
    "Config": SimpleNamespace(ERROR_TOPIC="errors"),
}
exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
Coordinator = namespace["CommitCoordinator"]
coordinator = Coordinator(None)
coordinator.init_tp("tp", 0)
coordinator.mark_done("tp", 0)
coordinator.try_commit(force=True)
coordinator.try_commit(force=True)
results = {"failed_commit_candidate_lost": len(calls) == 1 and not coordinator.done["tp"]}
calls.clear()
coordinator = Coordinator(None)
coordinator.init_tp("tp", 0)
coordinator.mark_done("tp", 0)
coordinator.mark_done("tp", 2)
coordinator.try_commit(force=True)
coordinator.try_commit(force=True)
results["numeric_offset_gap_stalls"] = calls == [{"tp": 1}] and coordinator.done["tp"] == {2}
calls.clear()
coordinator = Coordinator(None)
coordinator.mark_done("tp", 1)  # Malformed later input before queued earlier input dispatch.
coordinator.init_tp("tp", 0)
coordinator.try_commit(force=True)
results["later_parse_error_can_commit_earlier_unfinished_record"] = calls == [{"tp": 2}]
spec = SimpleNamespace(retry_to_dlq=None, name="worker", topics_out=["out"])
results["failed_default_dlq_send_treated_as_success"] = (
    namespace["_handle_retry_to_dlq"](None, spec, "in", {"id": "x"}, label_prefix="probe") is True
)


class Future:
    def get(self, **kwargs):
        raise RuntimeError("broker delivery failed")


namespace["_send_async"] = lambda *a, **kw: Future()
results["single_output_success_before_ack"] = (
    namespace["_forward_result"](None, spec, {"id": "x"}, wait_for_acks=False, ack_timeout_sec=1)
    is True
)
import redis

client = redis.Redis(host="localhost", port=26379)
import uuid

key = "qf-probe-" + uuid.uuid4().hex
lua = next(
    ast.literal_eval(n.value)
    for n in tree.body
    if isinstance(n, ast.Assign)
    and any(isinstance(t, ast.Name) and t.id == "_AGG_LUA" for t in n.targets)
)
try:
    client.eval(lua, 1, key, "a", '{"a":1}', 60, 2)
    one = client.eval(lua, 1, key, "b", '{"b":1}', 60, 2)
    two = client.eval(lua, 1, key, "a", '{"a":1}', 60, 2)
    results["aggregation_duplicate_ready_without_claim"] = bool(one) and bool(two)
finally:
    client.delete(key)
    client.close()
print(
    json.dumps(
        {
            "source": str(source),
            "method": "AST-selected original code; injected failed transport boundary; real Redis Lua",
            "defects_reproduced": results,
        },
        indent=2,
    )
)
