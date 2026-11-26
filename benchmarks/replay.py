import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import datetime
import http.client
import json
import math
from pathlib import Path
import time
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Request:
    arrival_s: float
    request_class: str
    prompt: str
    max_tokens: int


def workload(scenario, profile_words=(32, 128), profile_caps=(12, 24, 96)):
    if scenario == "calibration":
        return [
            Request(0, "interactive" if tokens == 12 else "batch",
                    ("Summarize this passage." + " word" * words), tokens)
            for words in profile_words
            for tokens in profile_caps
            for _ in range(3)
        ]

    def chat(arrival, index):
        return Request(arrival, "interactive", f"Question {index}: Why is the sky blue?", 12)

    def batch(arrival, index):
        prompt = (
            f"Record {index}: Explain the history of one computing invention. "
            "Include its technical design, practical use, and long-term influence."
        )
        return Request(arrival, "batch", prompt, 96)

    if scenario == "solo":
        return [chat(index * 0.7, index) for index in range(20)]
    if scenario == "batch-only":
        return [batch(index * 0.1, index) for index in range(8)]
    if scenario == "steady":
        requests = [chat(index * 0.6, index) for index in range(24)]
        requests += [batch(index * 2.3, index) for index in range(6)]
    elif scenario == "burst":
        requests = [
            chat(0.8 + group * 3.0 + offset * 0.08, group * 6 + offset)
            for group in range(4)
            for offset in range(6)
        ]
        requests += [batch(index * 0.85, index) for index in range(12)]
    else:
        requests = [chat(index * 0.14, index) for index in range(48)]
        requests += [batch(index * 0.35, index) for index in range(14)]
    return sorted(requests, key=lambda request: request.arrival_s)


def trace_workload(path, limit, offset):
    with path.open(newline="") as file:
        rows = sorted(
            (datetime.fromisoformat(row["TIMESTAMP"].replace("Z", "+00:00")),
             int(row["ContextTokens"]), int(row["GeneratedTokens"]))
            for row in csv.DictReader(file)
        )
    rows = [(when, prompt, output) for when, prompt, output in rows if output > 1]
    rows = rows[offset:offset + limit]
    if not rows:
        raise ValueError("trace has no usable requests")
    start = rows[0][0]
    chat = [
        Request((when - start).total_seconds(), "interactive",
                f"Request {i}:" + " word" * min(512, prompt), min(128, output))
        for i, (when, prompt, output) in enumerate(rows)
    ]
    background = [request for request in workload("steady") if request.request_class == "batch"]
    return sorted(chat + background, key=lambda request: request.arrival_s)


def send(endpoint, model, request, start, timeout_s, backend, native_priority):
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=timeout_s)
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": request.prompt}],
        "stream": True,
        "temperature": 0,
        "max_tokens": request.max_tokens,
        "ignore_eos": True,
        "stream_options": {"include_usage": True},
    }
    if backend == "llama":
        payload["cache_prompt"] = False
    if native_priority:
        payload["priority"] = 0 if request.request_class == "interactive" else 10
    payload = json.dumps(payload)
    sent = time.monotonic()
    result = {
        **asdict(request),
        "launch_delay_ms": (sent - start - request.arrival_s) * 1_000,
        "ttft_ms": None,
        "tpot_ms": None,
        "completion_tokens": None,
        "prompt_tokens": None,
        "total_ms": None,
        "queue_wait_ms": None,
        "borrowed": False,
        "status": None,
        "outcome": "incomplete",
    }

    try:
        connection.request(
            "POST",
            endpoint.path,
            body=payload,
            headers={"Content-Type": "application/json", "X-Request-Class": request.request_class},
        )
        response = connection.getresponse()
        result["status"] = response.status
        result["borrowed"] = response.getheader("X-Admission-Borrowed") == "1"
        queue_wait = response.getheader("X-Queue-Wait-Ms")
        if queue_wait is not None:
            result["queue_wait_ms"] = float(queue_wait)
        if response.status != 200:
            result["outcome"] = "http_error"
            return result

        first_text = last_text = None
        for line in response:
            now = time.monotonic()
            if not line.startswith(b"data:"):
                continue
            event = line[5:].strip()
            if event == b"[DONE]":
                if result["ttft_ms"] is not None:
                    result["outcome"] = "completed"
                break
            data = json.loads(event)
            usage = data.get("usage")
            if usage:
                result["completion_tokens"] = usage.get("completion_tokens")
                result["prompt_tokens"] = usage.get("prompt_tokens")
            choices = data.get("choices", [])
            if choices and choices[0].get("delta", {}).get("content"):
                if first_text is None:
                    first_text = now
                    result["ttft_ms"] = (now - sent) * 1_000
                last_text = now
        tokens = result["completion_tokens"]
        if first_text is not None and tokens is not None and tokens > 1:
            result["tpot_ms"] = (last_text - first_text) * 1_000 / (tokens - 1)
        return result
    except (OSError, ValueError, http.client.HTTPException) as error:
        result["outcome"] = "error"
        result["error"] = type(error).__name__
        return result
    finally:
        finished = time.monotonic()
        result["finished_s"] = finished - start
        result["total_ms"] = (finished - sent) * 1_000
        connection.close()


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def summarize(results, deadline_ms, tpot_ms=None):
    elapsed_s = max(result["finished_s"] for result in results)
    classes = {}
    for request_class in ("interactive", "batch"):
        offered = [result for result in results if result["request_class"] == request_class]
        completed = [result for result in offered if result["outcome"] == "completed"]
        starts = [result["ttft_ms"] for result in completed]
        queues = [result["queue_wait_ms"] for result in completed if result["queue_wait_ms"] is not None]
        speeds = [result["tpot_ms"] for result in completed if result["tpot_ms"] is not None]
        hits = None
        if deadline_ms is not None:
            hits = sum(
                result["ttft_ms"] <= deadline_ms
                and (tpot_ms is None or (result["tpot_ms"] is not None and result["tpot_ms"] <= tpot_ms))
                for result in completed
            )
        batch_tokens = sum(result["completion_tokens"] or 0 for result in completed)
        classes[request_class] = {
            "offered": len(offered),
            "borrowed_dispatches": sum(result["borrowed"] for result in offered),
            "completed": len(completed),
            "overload_503": sum(result["status"] == 503 for result in offered),
            "ttft_p95_ms": percentile(starts, 0.95),
            "tpot_p95_ms": percentile(speeds, 0.95),
            "completion_token_coverage": sum(result["completion_tokens"] is not None for result in completed),
            "completed_tokens_per_second": batch_tokens / elapsed_s,
            "queue_wait_p95_ms": percentile(queues, 0.95),
            "deadline_hit_fraction": hits / len(offered) if hits is not None and offered else None,
            "completed_per_second": len(completed) / elapsed_s,
        }
    return {
        "elapsed_s": elapsed_s,
        "launch_delay_p95_ms": percentile([result["launch_delay_ms"] for result in results], 0.95),
        **classes,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080/v1/chat/completions")
    parser.add_argument("--model", default="local-model")
    parser.add_argument("--label", required=True)
    parser.add_argument("--scenario", choices=("solo", "steady", "burst", "overload", "batch-only", "calibration"), required=True)
    parser.add_argument("--deadline-ms", type=float)
    parser.add_argument("--tpot-ms", type=float)
    parser.add_argument("--chat-scale", type=float, default=1)
    parser.add_argument("--batch-scale", type=float, default=1)
    parser.add_argument("--backend", choices=("llama", "vllm"), default="llama")
    parser.add_argument("--native-priority", action="store_true")
    parser.add_argument("--batch-max-tokens", type=int)
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--trace-limit", type=int, default=24)
    parser.add_argument("--trace-offset", type=int, default=0)
    parser.add_argument("--profile-words", nargs="+", type=int, default=[32, 128])
    parser.add_argument("--profile-caps", nargs="+", type=int, default=[12, 24, 96])
    parser.add_argument("--timeout-s", type=float, default=90)
    parser.add_argument("--allow-all-rejected", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.batch_max_tokens is not None and args.batch_max_tokens < 1:
        parser.error("--batch-max-tokens must be positive")
    endpoint = urlsplit(args.url)
    if endpoint.scheme != "http" or not endpoint.hostname or not endpoint.path:
        parser.error("--url must be an HTTP URL with a hostname and completions path")
    if args.tpot_ms is not None and args.deadline_ms is None:
        parser.error("--tpot-ms requires --deadline-ms")
    if args.native_priority and args.backend != "vllm":
        parser.error("--native-priority requires --backend vllm")
    if args.trace_limit < 1 or args.trace_offset < 0:
        parser.error("trace limit must be positive and offset nonnegative")
    for name, value in (
        ("--deadline-ms", args.deadline_ms), ("--timeout-s", args.timeout_s),
        ("--tpot-ms", args.tpot_ms), ("--chat-scale", args.chat_scale), ("--batch-scale", args.batch_scale),
    ):
        if value is not None and (not math.isfinite(value) or value <= 0):
            parser.error(f"{name} must be positive and finite")

    if min(args.profile_words + args.profile_caps) < 1 or 12 not in args.profile_caps:
        parser.error("profile buckets must be positive and include the 12-token chat case")
    if args.trace and args.scenario == "calibration":
        parser.error("calibration uses synthetic size buckets")
    base = (
        trace_workload(args.trace, args.trace_limit, args.trace_offset)
        if args.trace else workload(args.scenario, args.profile_words, args.profile_caps)
    )
    if args.batch_max_tokens is not None:
        if args.scenario == "calibration":
            parser.error("use --profile-caps to set calibration output limits")
        base = [
            replace(request, max_tokens=args.batch_max_tokens) if request.request_class == "batch" else request
            for request in base
        ]
    scales = {"interactive": args.chat_scale, "batch": args.batch_scale}
    requests = sorted(
        (replace(request, arrival_s=request.arrival_s * scales[request.request_class]) for request in base),
        key=lambda request: request.arrival_s,
    )
    if args.scenario == "calibration":
        start = time.monotonic()
        results = [
            send(endpoint, args.model, replace(request, arrival_s=time.monotonic() - start),
                 start, args.timeout_s, args.backend, args.native_priority)
            for request in requests
        ]
    else:
        results = replay(endpoint, args, requests)

    summary = summarize(results, args.deadline_ms, args.tpot_ms)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({
        "label": args.label,
        "scenario": args.scenario,
        "model": args.model,
        "deadline_ms": args.deadline_ms,
        "tpot_ms": args.tpot_ms,
        "chat_scale": args.chat_scale,
        "batch_scale": args.batch_scale,
        "batch_max_tokens": args.batch_max_tokens,
        "backend": args.backend,
        "native_priority": args.native_priority,
        "trace": args.trace.name if args.trace else None,
        "trace_offset": args.trace_offset if args.trace else None,
        "trace_limit": args.trace_limit if args.trace else None,
        "summary": summary,
        "requests": results,
    }, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    if not any(result["outcome"] == "completed" for result in results) and not (
        args.allow_all_rejected and all(result["status"] in (429, 503) for result in results)
    ):
        raise SystemExit("No requests completed. Check the model server, URL, and model name.")


def replay(endpoint, args, requests):
    with ThreadPoolExecutor(max_workers=len(requests)) as executor:
        start = time.monotonic()
        futures = []
        for request in requests:
            time.sleep(max(0, start + request.arrival_s - time.monotonic()))
            futures.append(executor.submit(send, endpoint, args.model, request, start, args.timeout_s, args.backend, args.native_priority))
        return [future.result() for future in futures]

if __name__ == "__main__":
    main()
