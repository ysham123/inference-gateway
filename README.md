# Inference Gateway

A local Rust gateway for streaming model responses. The first milestone forwards chat completions to a local model server as they arrive.

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
