import argparse
from collections import defaultdict
import json
import math
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("calibration", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--service-margin", type=float, default=1.5)
    args = parser.parse_args()
    if not math.isfinite(args.service_margin) or args.service_margin < 1:
        parser.error("--service-margin must be finite and at least 1")
    report = json.loads(args.calibration.read_text())
    if report["scenario"] != "calibration":
        parser.error("expected a serial calibration report")
    classes = defaultdict(list)
    chat_starts = []
    for request in report["requests"]:
        if request["outcome"] != "completed":
            parser.error("every calibration request must complete")
        key = (len(request["prompt"].encode()), request["max_tokens"])
        classes[key].append(request["total_ms"])
        if request["request_class"] == "interactive":
            chat_starts.append(request["ttft_ms"])
    if not chat_starts or any(len(times) < 3 for times in classes.values()):
        parser.error("profile requires chat calibration and three runs per size class")
    profile = {
        "chat_start_ms": math.ceil(max(chat_starts)),
        "classes": [
            {"prompt_bytes": size, "max_tokens": cap,
             "service_ms": math.ceil(max(times) * args.service_margin)}
            for (size, cap), times in sorted(classes.items())
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(profile, indent=2) + "\n")


if __name__ == "__main__":
    main()
