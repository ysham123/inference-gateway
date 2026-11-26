# Inference Gateway

A local Rust gateway for streaming chat completions with FIFO and weighted scheduling.

## Run locally

Start `llama-server` with a local GGUF model in one terminal:

```sh
llama-server --model /path/to/model.gguf --alias local-model --host 127.0.0.1 --port 8081
```

In a second terminal, run `cargo run` from this project folder. Send a streaming request in a third terminal:

```sh
curl --no-buffer http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"local-model","messages":[{"role":"user","content":"Write one sentence about Rust."}],"stream":true,"max_tokens":40}'
```

You should see separate `data:` events as text arrives and a final `data: [DONE]` event. The gateway health endpoint at `/health` returns `204` when the gateway is running.

Set `MODEL_SERVER_URL` to point to another local HTTP server that exposes `/v1/chat/completions`. For Ollama, run `MODEL_SERVER_URL=http://127.0.0.1:11434 cargo run` and use a model name shown by `ollama list` in the request. Set `GATEWAY_ADDR` to change where the gateway listens.

## Request limits

The gateway allows two active requests and queues up to 32 more by default. An active request holds its slot until its stream ends or is dropped. The gateway returns HTTP 503 when the queue is full.

Set `MAX_ACTIVE_REQUESTS` to a positive value that fits the backend's parallel capacity. Set `MAX_QUEUED_REQUESTS` to change the waiting limit. A waiting limit of zero rejects requests immediately when every slot is busy.

## Scheduling

`SCHEDULING_POLICY=fifo` serves waiting requests in arrival order and is the default. `SCHEDULING_POLICY=weighted` dispatches three interactive requests for every batch request when both classes have work waiting. Each class keeps its own arrival order, and either class can use spare capacity when the other is empty.

Send `X-Request-Class: batch` for batch work. Requests without the header are interactive. The `X-Queue-Wait-Ms` response header measures time spent waiting for gateway admission.

## Replay a workload

The Python benchmark sends requests on a fixed schedule, even when earlier requests are still running. It expects a local `llama-server` and uses fixed generation lengths with prompt caching disabled. Start the backend with a fixed `--parallel` value (two for these examples), and match it with `MAX_ACTIVE_REQUESTS` on the gateway.

```sh
python3 benchmarks/replay.py --label solo --scenario solo --output /tmp/gateway-solo.json
python3 benchmarks/replay.py --label fifo --scenario burst --deadline-ms 750 --output /tmp/gateway-fifo.json
```

Choose a first-token target from the solo run before comparing policies. Use the same target and scenario for each comparison. Run against the backend directly by setting `--url http://127.0.0.1:8081/v1/chat/completions`.

The JSON report has one row per offered request. The time-to-first-token p95 describes successful streams, and the deadline hit fraction includes failures and rejected requests in the denominator. Batch throughput counts completed requests across the full replay window, including time to drain the queue.
