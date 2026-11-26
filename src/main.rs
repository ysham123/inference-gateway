mod scheduler;

use std::{env, error::Error, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Url, redirect::Policy};
use scheduler::Scheduler;
use tokio::net::TcpListener;

#[derive(Clone)]
struct Gateway {
    client: Client,
    completions_url: Url,
    scheduler: Arc<Scheduler>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = env::var("GATEWAY_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    let upstream = env::var("MODEL_SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:8081".into());
    let max_active: NonZeroUsize = env::var("MAX_ACTIVE_REQUESTS")
        .unwrap_or_else(|_| "2".into())
        .parse()?;
    let max_waiting: usize = env::var("MAX_QUEUED_REQUESTS")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
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
            scheduler: Arc::new(Scheduler::new(max_active, max_waiting)),
        });

    let listener = TcpListener::bind(address).await?;
    println!("Gateway listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn chat_completions(State(gateway): State<Gateway>, body: Bytes) -> Response {
    let Ok(admission) = gateway.scheduler.acquire().await else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Model queue is full").into_response();
    };

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
    let stream = stream::unfold(
        (Box::pin(upstream.bytes_stream()), admission),
        |(mut upstream, admission)| async move {
            let chunk = upstream.next().await?;
            Some((chunk, (upstream, admission)))
        },
    );
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
}
