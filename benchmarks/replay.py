import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
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


def workload(scenario):
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


def send(endpoint, model, request, start, timeout_s):
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=timeout_s)
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": request.prompt}],
        "stream": True,
        "temperature": 0,
        "max_tokens": request.max_tokens,
        "ignore_eos": True,
        "cache_prompt": False,
    })
    sent = time.monotonic()
    result = {
        **asdict(request),
        "launch_delay_ms": (sent - start - request.arrival_s) * 1_000,
        "ttft_ms": None,
        "queue_wait_ms": None,
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
        queue_wait = response.getheader("X-Queue-Wait-Ms")
        if queue_wait is not None:
            result["queue_wait_ms"] = float(queue_wait)
        if response.status != 200:
            result["outcome"] = "http_error"
            return result

        for line in response:
            now = time.monotonic()
            if not line.startswith(b"data:"):
                continue
            event = line[5:].strip()
            if event == b"[DONE]":
                if result["ttft_ms"] is not None:
                    result["outcome"] = "completed"
                break
            choices = json.loads(event).get("choices", [])
            if choices and choices[0].get("delta", {}).get("content") and result["ttft_ms"] is None:
                result["ttft_ms"] = (now - sent) * 1_000
        return result
    except (OSError, ValueError, http.client.HTTPException) as error:
        result["outcome"] = "error"
        result["error"] = type(error).__name__
        return result
    finally:
        result["finished_s"] = time.monotonic() - start
        connection.close()


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def summarize(results, deadline_ms):
    elapsed_s = max(result["finished_s"] for result in results)
    classes = {}
    for request_class in ("interactive", "batch"):
        offered = [result for result in results if result["request_class"] == request_class]
        completed = [result for result in offered if result["outcome"] == "completed"]
        starts = [result["ttft_ms"] for result in completed]
        queues = [result["queue_wait_ms"] for result in completed if result["queue_wait_ms"] is not None]
        hits = sum(ms <= deadline_ms for ms in starts) if deadline_ms is not None else None
        classes[request_class] = {
            "offered": len(offered),
            "completed": len(completed),
            "overload_503": sum(result["status"] == 503 for result in offered),
            "ttft_p95_ms": percentile(starts, 0.95),
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
    parser.add_argument("--scenario", choices=("solo", "steady", "burst", "overload"), required=True)
    parser.add_argument("--deadline-ms", type=float)
    parser.add_argument("--timeout-s", type=float, default=90)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    endpoint = urlsplit(args.url)
    if endpoint.scheme != "http" or not endpoint.hostname or not endpoint.path:
        parser.error("--url must be an HTTP URL with a hostname and completions path")
    for name, value in (("--deadline-ms", args.deadline_ms), ("--timeout-s", args.timeout_s)):
        if value is not None and (not math.isfinite(value) or value <= 0):
            parser.error(f"{name} must be positive and finite")

    requests = workload(args.scenario)
    with ThreadPoolExecutor(max_workers=len(requests)) as executor:
        start = time.monotonic()
        futures = []
        for request in requests:
            time.sleep(max(0, start + request.arrival_s - time.monotonic()))
            futures.append(executor.submit(send, endpoint, args.model, request, start, args.timeout_s))
        results = [future.result() for future in futures]

    summary = summarize(results, args.deadline_ms)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({
        "label": args.label,
        "scenario": args.scenario,
        "model": args.model,
        "deadline_ms": args.deadline_ms,
        "summary": summary,
        "requests": results,
    }, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    if not any(result["outcome"] == "completed" for result in results):
        raise SystemExit("No requests completed. Check the model server, URL, and model name.")


if __name__ == "__main__":
    main()
