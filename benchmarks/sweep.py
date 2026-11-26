import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen


@contextmanager
def gateway(args, policy):
    if policy == "direct":
        yield args.backend_url
        return

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    address = f"{host}:{port}"
    endpoint = urlsplit(args.backend_url)
    env = {
        **os.environ,
        "GATEWAY_ADDR": address,
        "MODEL_SERVER_URL": f"{endpoint.scheme}://{endpoint.netloc}",
        "SCHEDULING_POLICY": policy,
        "MAX_ACTIVE_REQUESTS": str(args.slots),
        "MAX_QUEUED_REQUESTS": str(args.queue),
        "ADMISSION_PROFILE": str(args.profile.resolve()),
        "CHAT_TTFT_MS": str(args.deadline_ms),
    }
    with subprocess.Popen([str(args.gateway.resolve())], env=env) as proc:
        try:
            for _ in range(50):
                if proc.poll() is not None:
                    raise RuntimeError(f"{policy} exited before becoming healthy")
                try:
                    with urlopen(f"http://{address}/health", timeout=0.2) as response:
                        if response.status == 204:
                            break
                except (OSError, URLError):
                    time.sleep(0.1)
            else:
                raise RuntimeError(f"{policy} gateway did not become healthy")
            yield f"http://{address}/v1/chat/completions"
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--backend-url", required=True)
    parser.add_argument("--backend", choices=("llama", "vllm"), required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--deadline-ms", type=int, required=True)
    parser.add_argument("--tpot-ms", type=float, required=True)
    parser.add_argument("--slots", type=int, default=2)
    parser.add_argument("--queue", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--chat-scales", type=float, nargs="+", default=[0.5, 1, 2])
    parser.add_argument("--batch-scales", type=float, nargs="+", default=[0.5, 1, 2])
    parser.add_argument("--scenario", choices=("steady", "burst", "overload"), default="steady")
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--trace-offset", type=int, default=0)
    args = parser.parse_args()
    if args.slots < 2 or args.queue < 0 or args.repeats < 1:
        parser.error("need at least two slots, a nonnegative queue, and one or more repeats")
    if not args.profile.is_file() or not args.gateway.is_file():
        parser.error("provide a calibrated profile and a built gateway binary")

    policies = ["direct", "weighted", "reserved", "adaptive"]
    best = {policy: None for policy in policies}
    args.out.mkdir(parents=True, exist_ok=True)
    for chat_scale in args.chat_scales:
        for batch_scale in args.batch_scales:
            results = {policy: [] for policy in policies}
            for repeat in range(args.repeats):
                order = policies[repeat % len(policies):] + policies[:repeat % len(policies)]
                for policy in order:
                    name = f"{policy}-c{chat_scale}-b{batch_scale}-r{repeat}"
                    path = args.out / f"{name}.json"
                    with gateway(args, policy) as url:
                        command = [
                            sys.executable, str(Path(__file__).with_name("replay.py")),
                            "--url", url, "--model", args.model, "--backend", args.backend,
                            "--allow-all-rejected", "--label", name, "--scenario", args.scenario, "--output", str(path),
                            "--deadline-ms", str(args.deadline_ms), "--tpot-ms", str(args.tpot_ms),
                            "--chat-scale", str(chat_scale), "--batch-scale", str(batch_scale),
                        ]
                        if args.backend == "vllm":
                            command.append("--native-priority")
                        if args.trace:
                            command += ["--trace", str(args.trace), "--trace-offset", str(args.trace_offset)]
                        subprocess.run(command, check=True)
                    results[policy].append(json.loads(path.read_text())["summary"])
            for policy, runs in results.items():
                batch_runs = [run["batch"] for run in runs]
                if all(
                    run["interactive"]["deadline_hit_fraction"] >= 0.95
                    and batch["completed"] == batch["completion_token_coverage"]
                    for run, batch in zip(runs, batch_runs)
                ):
                    rate = min(batch["completed_tokens_per_second"] for batch in batch_runs)
                    if best[policy] is None or rate > best[policy]["batch_tokens_per_second"]:
                        best[policy] = {
                            "chat_scale": chat_scale, "batch_scale": batch_scale,
                            "batch_tokens_per_second": rate,
                        }
    (args.out / "frontier.json").write_text(json.dumps(best, indent=2) + "\n")


if __name__ == "__main__":
    main()
