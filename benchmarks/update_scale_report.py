"""Render explicit 1k/100k timing comparisons from raw trials, including partial runs."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = {
    "noop": "Identity",
    "io": "5 ms I/O / message",
    "bulk": "32-record bulk",
    "retry": "10% transient retry",
}
ENGINES = ("original_ack", "async")
START = "<!-- SCALE_COMPARISON_START -->"
END = "<!-- SCALE_COMPARISON_END -->"


def grouped(data):
    result = defaultdict(list)
    for trial in data["trials"]:
        result[trial["workload"], trial["engine"]].append(trial)
    return result


def median(trials, key):
    values = [trial[key] for trial in trials if "error" not in trial and key in trial]
    return statistics.median(values) if values else None


def fmt(value, precision=2):
    return "—" if value is None else f"{value:,.{precision}f}"


def passed(trials):
    return f"{sum('error' not in trial for trial in trials)}/{len(trials)}"


def markdown_table(headers, rows):
    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
            *["| " + " | ".join(map(str, row)) + " |" for row in rows],
        ]
    )


def render(small, large, details=False):
    data = {1000: grouped(small), 100000: grouped(large)}
    complete = all(
        len(data[100000][workload, engine]) == 3 for workload in WORKLOADS for engine in ENGINES
    )
    count = len(large["trials"])
    status = (
        "Complete: 24 of 24 attempted trials recorded."
        if complete
        else f"In progress: {count} of 24 planned trials recorded. All 100,000-message numbers are preliminary until three repetitions per engine/workload finish."
    )
    rows = []
    commit_rows = []
    ratios = {}
    for workload, label in WORKLOADS.items():
        for size in (1000, 100000):
            original, native = (data[size][workload, engine] for engine in ENGINES)
            old_rate, new_rate = (
                median(original, "messages_per_sec"),
                median(native, "messages_per_sec"),
            )
            ratio = new_rate / old_rate if old_rate is not None and new_rate is not None else None
            ratios[workload, size] = ratio
            rows.append(
                [
                    label,
                    f"{size:,}",
                    fmt(median(original, "elapsed_sec")),
                    fmt(median(native, "elapsed_sec")),
                    fmt(old_rate),
                    fmt(new_rate),
                    fmt(ratio) + "×" if ratio is not None else "—",
                    passed(original),
                    passed(native),
                ]
            )
            commit_rows.append(
                [
                    label,
                    f"{size:,}",
                    fmt(median(original, "commit_elapsed_sec")),
                    fmt(median(native, "commit_elapsed_sec")),
                    fmt(median(original, "commit_messages_per_sec")),
                    fmt(median(native, "commit_messages_per_sec")),
                ]
            )
    text = "## Processing 1,000 versus 100,000 messages\n\n" + status + "\n\n"
    text += "Same four partitions, eight worker jobs and three repetitions per engine/workload. The comparator is the **original ACK-gated harness variant**; it changes single-output waiting without repairing the original's other reliability defects. Output time measures the complete input burst to final observed output. Throughput is messages/second; higher is better. Time is seconds; lower is better.\n\n"
    text += markdown_table(
        [
            "Workload",
            "Messages",
            "Flask ACK output s",
            "Async output s",
            "Flask ACK msg/s",
            "Async msg/s",
            "Async / Flask rate",
            "Flask passed/attempted",
            "Async passed/attempted",
        ],
        rows,
    )
    text += "\n\nEach cell is a median across successful trials. `0/0` means not yet attempted; failed attempts remain in the success counts and raw data. A ratio of 2.00× means twice the throughput, approximately half the elapsed time for the same count; a ratio below 1.00× favors Flask.\n\n### Source-commit completion time\n\n"
    text += "This separately waits until the broker reports source offsets committed through the published inputs. It is not atomic output-plus-input delivery or an exactly-once guarantee.\n\n"
    text += markdown_table(
        [
            "Workload",
            "Messages",
            "Flask ACK commit s",
            "Async commit s",
            "Flask ACK commit msg/s",
            "Async commit msg/s",
        ],
        commit_rows,
    )
    if details:
        text += "\n\n### Does the small-test advantage hold at 100,000 messages?\n\n"
        ratio_rows = []
        for workload, label in WORKLOADS.items():
            old, new = ratios[workload, 1000], ratios[workload, 100000]
            original, native = (data[100000][workload, engine] for engine in ENGINES)
            if new is None:
                verdict = "Pending paired successful trials"
            elif len(original) < 3 or len(native) < 3:
                verdict = "Preliminary; repetitions still running"
            elif any("error" in t for t in original + native):
                verdict = "Successful-trial comparison only; inspect failures"
            elif new >= old:
                verdict = "Small-test advantage retained or increased"
            elif new > 1:
                verdict = "Async remains faster; advantage is smaller at scale"
            else:
                verdict = "Small-test advantage does not hold"
            by_repeat = {
                engine: {t["repeat"]: t for t in data[100000][workload, engine] if "error" not in t}
                for engine in ENGINES
            }
            paired = [
                by_repeat["async"][repeat]["messages_per_sec"] / trial["messages_per_sec"]
                for repeat, trial in by_repeat["original_ack"].items()
                if repeat in by_repeat["async"]
            ]
            observed = f"{min(paired):.2f}–{max(paired):.2f}×" if paired else "—"
            ratio_rows.append(
                [
                    label,
                    fmt(old) + "×",
                    fmt(new) + "×" if new is not None else "—",
                    observed,
                    verdict,
                ]
            )
        text += markdown_table(
            [
                "Workload",
                "1,000-message ratio",
                "100,000-message ratio",
                "100k paired-run range",
                "Interpretation",
            ],
            ratio_rows,
        )
        if (
            len(data[100000]["noop", "original_ack"]) == 3
            and len(data[100000]["noop", "async"]) == 3
        ):
            small_original = median(data[1000]["noop", "original_ack"], "messages_per_sec")
            large_original = median(data[100000]["noop", "original_ack"], "messages_per_sec")
            small_async = median(data[1000]["noop", "async"], "messages_per_sec")
            large_async = median(data[100000]["noop", "async"], "messages_per_sec")
            text += (
                f"\n\nIdentity's relative gain falls because original throughput rises from "
                f"{small_original:,.0f} to {large_original:,.0f} msg/s, while async stays near "
                f"{small_async:,.0f} to {large_async:,.0f} msg/s. A smaller ratio therefore does "
                "not mean async throughput collapsed; the original benefits more from the larger burst.\n"
            )

        text += "\n\nRatios above divide throughput medians. The paired-run range separately divides the async and original rates within each repetition; it is an observed range, not a statistical confidence interval. A wide range limits how confidently a single multiplier can be extrapolated.\n"
        text += "\n\n### 100,000-message latency and process resources\n\n"
        detail_rows = []
        for (workload, engine), trials in data[100000].items():
            rates = [t["messages_per_sec"] for t in trials if "error" not in t]
            rate_range = f"{min(rates):,.2f}–{max(rates):,.2f}" if rates else "—"
            detail_rows.append(
                [
                    WORKLOADS[workload],
                    engine,
                    passed(trials),
                    rate_range,
                    fmt(median(trials, "p50_ms")),
                    fmt(median(trials, "p95_ms")),
                    fmt(median(trials, "p99_ms")),
                    fmt(median(trials, "cpu_sec")),
                    fmt(median(trials, "peak_rss_mib")),
                ]
            )
        text += markdown_table(
            [
                "Workload",
                "Engine",
                "Passed/attempted",
                "Throughput range msg/s",
                "p50 ms",
                "p95 ms",
                "p99 ms",
                "CPU s",
                "RSS MiB",
            ],
            detail_rows,
        )
        errors = [trial for trial in large["trials"] if "error" in trial]
        text += "\n\n### Completion and resilience observations\n\n"
        good = [t for t in large["trials"] if "error" not in t]
        text += f"{len(good)} completed 100,000-message trials and {len(errors)} incomplete attempts are currently recorded. Completed trials contain {sum(t.get('missing', 0) for t in good)} missing outputs, {sum(t.get('duplicates', 0) for t in good)} immediate duplicates and {sum(t.get('dlq', 0) for t in good)} DLQ records. This is happy-path stress evidence; it does not replace the separate crash/outage tests.\n\n"
        if errors:
            text += (
                markdown_table(
                    [
                        "Engine",
                        "Workload",
                        "Repeat",
                        "Phase",
                        "Waiting for",
                        "Unique outputs",
                        "Missing outputs",
                        "Failure",
                    ],
                    [
                        [
                            t["engine"],
                            t["workload"],
                            t["repeat"],
                            t.get("benchmark_phase", "unknown"),
                            t.get("stage", "unknown"),
                            t.get("unique_received", "unknown"),
                            t.get("missing", "unknown"),
                            t["error"],
                        ]
                        for t in errors
                    ],
                )
                + "\n\n"
            )
        text += "The large-run driver uses incremental unique-ID tracking to avoid repeated large set reconstruction. The payload, workers and measurement boundaries are unchanged; it also emits progress and records explicit output/source-commit stall diagnostics. Per-phase overall deadline is 900 seconds; no-progress deadline is 60 seconds. Execution order alternates across repetitions. Warmup is 128 records, excluded from measurement. Single broker, loopback transport and finite bursts remain the main experimental limits.\n"
    return text


def replace_section(text, section, before):
    block = START + "\n" + section + "\n" + END
    if START in text:
        return re.sub(re.escape(START) + ".*?" + re.escape(END), lambda _: block, text, flags=re.S)
    return text.replace(before, block + "\n\n" + before, 1)


def main(parent_report=None):
    small = json.loads((ROOT / "benchmarks/results/pipelines.json").read_text())
    large = json.loads((ROOT / "benchmarks/results/pipelines_100k.json").read_text())
    report = ROOT / "docs/qf_flask_vs_qf_fastapi_report.md"
    text = replace_section(
        report.read_text(),
        render(small, large, details=True),
        "## Larger bursts, tuning and failures",
    )
    if len(large["trials"]) == 24:
        text = text.replace(
            "The matched 100,000-message comparison is running with the same four workloads, four partitions and eight-worker concurrency, using `original_ack` and `async`, three repetitions each. Results will appear in `pipelines_100k.json` and a dedicated section below. **The 1,000-message ratios are not yet claims about 100,000 messages.** The driver now records whether incomplete runs stalled on output or source commits; deadlines and failed-trial counts will be reported explicitly.",
            "The completed 100,000-message comparison uses the same four workloads, four partitions and eight-worker concurrency, with `original_ack` and `async`, three repetitions each. The side-by-side processing-time, throughput and ratio tables below distinguish small-burst gains from sustained large-burst behavior and retain every incomplete trial.",
        )
    report.write_text(text)
    readme = ROOT / "README.md"
    text = readme.read_text()
    text = text.replace(
        "The 100,000-message timing and ratio tables will be added when its three repetitions per workload complete; early identity results already show the small-burst ratio should not be extrapolated.\n",
        "",
    )
    text = replace_section(text, render(small, large), "### HTTP · 1,000 requests per trial")
    readme.write_text(text)
    if parent_report:
        text = report.read_text().replace("../benchmarks/", "qf_framework_fastapi/benchmarks/")
        for filename in ["Migration.md", "Tracing.md"]:
            text = text.replace(f"]({filename})", f"](qf_framework_fastapi/docs/{filename})")
        Path(parent_report).write_text(text)
    print(f"Updated comparison tables from {len(large['trials'])}/24 large trials.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parent-report", help="Optional explicitly requested workspace report copy"
    )
    args = parser.parse_args()
    main(args.parent_report)
