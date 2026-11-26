# Inference Gateway

A local Rust gateway for streaming chat completions with first-come, first-served scheduling.

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
