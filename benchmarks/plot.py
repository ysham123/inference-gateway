import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--joint", action="store_true")
    args = parser.parse_args()

    reports = [json.loads(path.read_text()) for path in args.reports]
    cases = {
        (report["scenario"], report["deadline_ms"], report["model"], report.get("tpot_ms"),
         report.get("chat_scale", 1), report.get("batch_scale", 1))
        for report in reports
    }
    if len(cases) != 1 or reports[0]["deadline_ms"] is None:
        parser.error("all reports must use the same model, scenario, and first-token target")
    scenario, target, _, tpot, _, _ = cases.pop()
    if args.joint != (tpot is not None):
        parser.error("--joint is required for reports with both deadlines and omitted for first-token-only reports")

    labels = [
        "FIFO" if report["label"] == "fifo" else report["label"].capitalize()
        for report in reports
    ]
    color_by_policy = {
        "direct": "#667085",
        "fifo": "#405CB3",
        "weighted": "#7661BA",
        "reserved": "#137D74",
    }
    colors = [color_by_policy.get(report["label"], "#667085") for report in reports]
    hit_rates = [100 * report["summary"]["interactive"]["deadline_hit_fraction"] for report in reports]
    if args.joint:
        batch_rates = [report["summary"]["batch"]["completed_tokens_per_second"] for report in reports]
        batch_units = "Completed output tokens per second"
        hit_title = "Chat met both latency targets"
    else:
        batch_rates = [60 * report["summary"]["batch"]["completed_per_second"] for report in reports]
        batch_units = "Completed requests per minute"
        hit_title = f"Chat started within {target / 1000:g} s"
    fig, axes = plt.subplots(1, 2, figsize=(8.8, 3.9), layout="constrained")
    panels = (
        (hit_rates, hit_title, "% of offered chat requests", "%.0f%%", 100),
        (batch_rates, "Batch throughput", batch_units, "%.1f", max(1, max(batch_rates) * 1.25)),
    )
    for ax, (values, title, axis_label, number_format, limit) in zip(axes, panels):
        bars = ax.bar(labels, values, color=colors, width=0.58, zorder=3)
        ax.bar_label(bars, fmt=number_format, padding=3, fontsize=9)
        ax.set_title(title, fontsize=11, loc="left", fontweight="bold", pad=9)
        ax.set_ylabel(axis_label, fontsize=9)
        ax.set_ylim(0, limit)
        ax.tick_params(axis="both", labelsize=9, length=0)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.spines["bottom"].set_color("#D0D5DD")
        ax.grid(axis="y", color="#E4E7EC", linewidth=0.8)
    fig.suptitle(
        f"Inference gateway | {scenario.capitalize()} workload",
        x=0.02, ha="left", fontsize=15, fontweight="bold",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=170, facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    main()
