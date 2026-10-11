from dataclasses import replace

import pytest

from framework.commons.utils import eval_file, deep_merge
from framework.commons.hex import i2b, h2k, lski
from framework.commons.apdu_utils import get_safe
from framework.decorators import kafka_handler, call_retry, retry_to_dlq
from framework.decorators.kafka_workers import all_workers, register_worker


def test_call_decorator_outside_registration_is_executed():
    calls = 0

    @call_retry(max_attempts=2, wait_fixed=0)
    @kafka_handler(name="outer", topics_in=["outer.in"], topics_out=["outer.out"])
    def worker(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError()
        return 42

    spec = next(s for s in all_workers() if s.name == "outer")
    assert spec.fn(None, "test", {}) == 42
    assert calls == 2


def test_policy_orders_and_worker_validation(spec):
    @retry_to_dlq(max_attempts=4, dlq_topic="errors")
    @kafka_handler(name="policy", topics_in=["policy.in"], topics_out=["policy.out"])
    def worker(*args):
        pass

    assert next(s for s in all_workers() if s.name == "policy").retry_to_dlq.max_attempts == 4
    with pytest.raises(ValueError):
        register_worker(replace(spec, max_workers=0))
    with pytest.raises(ValueError):
        register_worker(replace(spec, topics_in=["x", "x"]))


def test_literal_file_cannot_execute_code(tmp_path):
    path = tmp_path / "data"
    path.write_text("__import__('os').getcwd()")
    with pytest.raises(ValueError):
        eval_file(path)
    path.write_text("{'a': [1, 2]}")
    assert eval_file(path) == {"a": [1, 2]}


@pytest.mark.parametrize("value", [0, 1, 255, 256, 4095, 65536])
def test_integer_to_bytes_full_width(value):
    assert int.from_bytes(i2b(value), "big") == value


def test_nested_safe_empty_path_and_array():
    assert get_safe({"a": [{"b": 2}]}, "a", 0, "b") == 2
    assert get_safe({"a": 1}) == {"a": 1}
    assert get_safe([], 5, d="missing") == "missing"
    assert get_safe({"a": None}, "a", "b", d="missing") == "missing"


def test_hex_literal_parsing_and_merge_isolation():
    assert h2k("00 {'l':2} ff") == [{"l": 2}]
    assert lski("{'l':2}") == 2
    with pytest.raises((ValueError, SyntaxError)):
        h2k("{'x': __builtins__}")
    other = {"a": {"b": []}}
    result = deep_merge({}, other)
    result["a"]["b"].append(1)
    assert not other["a"]["b"]
