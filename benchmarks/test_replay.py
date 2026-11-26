from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from replay import summarize, trace_workload


class ReplayTests(unittest.TestCase):
    def test_failed_and_slow_streams_count_against_joint_objective(self):
        base = {
            "request_class": "interactive",
            "status": 200,
            "outcome": "completed",
            "ttft_ms": 20,
            "tpot_ms": 10,
            "completion_tokens": 12,
            "finished_s": 1,
            "queue_wait_ms": 0,
            "launch_delay_ms": 0,
            "borrowed": False,
        }
        rows = [
            base, base | {"tpot_ms": 200},
            base | {"status": 503, "outcome": "http_error", "completion_tokens": None},
        ]
        summary = summarize(rows, 100, 50)["interactive"]
        self.assertEqual(summary["deadline_hit_fraction"], 1 / 3)
        self.assertEqual(summary["completed_tokens_per_second"], 24)

    def test_trace_uses_a_contiguous_window_and_relative_arrivals(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "trace.csv"
            path.write_text(
                "TIMESTAMP,ContextTokens,GeneratedTokens\n"
                "2024-05-12 00:00:00.000+00:00,8,3\n"
                "2024-05-12 00:00:01.000+00:00,9,4\n"
                "2024-05-12 00:00:01.250+00:00,10,5\n"
            )
            chat = [request for request in trace_workload(path, 2, 1)
                    if request.request_class == "interactive"]
        self.assertEqual([(r.arrival_s, r.max_tokens) for r in chat], [(0, 4), (0.25, 5)])


if __name__ == "__main__":
    unittest.main()
