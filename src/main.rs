use std::{env, error::Error, net::SocketAddr, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::{Client, Url, redirect::Policy};
use tokio::net::TcpListener;

#[derive(Clone)]
struct Gateway {
    client: Client,
    completions_url: Url,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = env::var("GATEWAY_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    let upstream = env::var("MODEL_SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:8081".into());
    let completions_url = Url::parse(&upstream)?.join("/v1/chat/completions")?;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(120))
        .build()?;

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::NO_CONTENT }))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(Gateway {
            client,
            completions_url,
        });

    let listener = TcpListener::bind(address).await?;
    println!("Gateway listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn chat_completions(State(gateway): State<Gateway>, body: Bytes) -> Response {
    let upstream = gateway
        .client
        .post(gateway.completions_url)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;

    let upstream = match upstream {
        Ok(response) => response,
        Err(error) => {
            eprintln!("Model request failed: {error}");
            return (StatusCode::BAD_GATEWAY, "Could not reach the model server").into_response();
        }
    };

    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    // Axum sends each chunk as reqwest receives it from the model server.
    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
}
