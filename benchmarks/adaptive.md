# Adaptive admission evaluation

The gateway estimates service time from prompt bytes and the requested generation cap, using the smallest covering bucket from a serial calibration. The profile uses the slowest of repeated observations per bucket with a configurable margin. When batch would take the last idle slot, the gateway requires a profiled estimate for the candidate and all active requests. It compares the earliest predicted completion plus the profiled chat startup time with the chat budget. Unknown and overdue estimates preserve capacity. The gateway marks a borrowed dispatch with `X-Admission-Borrowed`.

These are request-level estimates subject to contention, burst arrivals and model behavior. ConServe studies the interference from already-running batch work and uses engine-level preemption. JITServe accounts for uncertainty when scheduling work. DistServe evaluates how much traffic an engine can serve within both first-token and decoding latency budgets. [ConServe](https://proceedings.mlr.press/v306/qiao26i.html) · [JITServe](https://www.usenix.org/conference/nsdi26/presentation/zhang-wei) · [DistServe](https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin)

## Reproduce

Use a warmed backend with a fixed model, engine version and concurrency settings. For CUDA validation use vLLM with `--scheduling-policy priority`. Set the same model alias for calibration and replay. The harness sends the same priority mapping in direct and gateway runs. [vLLM serve options](https://docs.vllm.ai/en/latest/cli/serve/#--scheduling-policy)

```sh
cargo build --release --locked
python3 benchmarks/replay.py --backend vllm --native-priority \
  --url http://127.0.0.1:8081/v1/chat/completions --model local-model \
  --label calibration --scenario calibration --profile-words 32 128 512 \
  --profile-caps 12 24 96 128 --output /tmp/gateway-calibration.json
python3 benchmarks/profile.py /tmp/gateway-calibration.json --output /tmp/gateway-profile.json
```

For a Mac pilot use `--backend llama` in both commands that contact the backend and omit `--native-priority` in calibration. Size the model context per slot to fit the longest calibration prompt and output. Replace the latency budgets with targets fixed before replay for the hardware being tested.

```sh
python3 benchmarks/sweep.py --backend vllm \
  --backend-url http://127.0.0.1:8081/v1/chat/completions --model local-model \
  --gateway target/release/inference-gateway --profile /tmp/gateway-profile.json \
  --deadline-ms 1400 --tpot-ms 150 --chat-scales 0.5 1 2 --batch-scales 0.5 1 2 \
  --repeats 3 --out /tmp/gateway-sweep
```

Use `--trace` with the Azure conversation CSV and different `--trace-offset` values for separate replay windows. The replay uses timestamps and approximate input sizes to generate synthetic prompts, clips prompt size and output caps, and adds a synthetic batch workload. Pick the windows in advance and preserve the backend, profile, settings and source offsets with the results. [Azure 2024 trace and citation information](https://github.com/Azure/AzurePublicDataset/blob/master/AzureLLMInferenceDataset2024.md)

A chat request meets the objective when the stream completes and both its first text and average time per output token are within budget. Rejections, transport errors and missing timing data count as misses. Batch throughput uses reported completion tokens and includes queue drain. `frontier.json` records the highest tested batch rate per policy whose repeats each achieve 95% chat attainment. Its rate is the lowest across those repeats. A null entry means the tested grid did not meet that condition. These are finite replays, so report the workload window and completion counts with the result.
