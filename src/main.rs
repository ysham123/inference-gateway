mod profile;
mod scheduler;

#[cfg(test)]
mod gateway_tests;

use std::{
    env,
    error::Error,
    net::SocketAddr,
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{StreamExt, stream};
use profile::Profile;
use reqwest::{Client, Url, redirect::Policy};
use scheduler::{RequestClass, Scheduler, SchedulingPolicy};
use tokio::net::TcpListener;

#[derive(Clone)]
struct Gateway {
    client: Client,
    completions_url: Url,
    scheduler: Arc<Scheduler>,
    profile: Option<Arc<Profile>>,
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
    let policy_name = env::var("SCHEDULING_POLICY").unwrap_or_else(|_| "fifo".into());
    let (policy, profile) = if policy_name == "adaptive" {
        let path = env::var("ADMISSION_PROFILE")?;
        let profile = Arc::new(Profile::load(Path::new(&path))?);
        let deadline_ms: u64 = env::var("CHAT_TTFT_MS")?.parse()?;
        let slack = Duration::from_millis(deadline_ms)
            .checked_sub(profile.chat_start)
            .filter(|slack| !slack.is_zero())
            .ok_or("CHAT_TTFT_MS must exceed the profiled chat start time")?;
        (SchedulingPolicy::Adaptive { slack }, Some(profile))
    } else {
        (policy_name.parse()?, None)
    };
    if matches!(
        policy,
        SchedulingPolicy::Reserved | SchedulingPolicy::Adaptive { .. }
    ) && max_active.get() < 2
    {
        return Err(
            "reserved and adaptive policies require MAX_ACTIVE_REQUESTS of at least 2".into(),
        );
    }
    let completions_url = Url::parse(&upstream)?.join("/v1/chat/completions")?;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(120))
        .build()?;

    let app = router(Gateway {
        client,
        completions_url,
        scheduler: Arc::new(Scheduler::new(max_active, max_waiting, policy)),
        profile,
    });

    let listener = TcpListener::bind(address).await?;
    println!("Gateway listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

fn router(gateway: Gateway) -> Router {
    Router::new()
        .route("/health", get(|| async { StatusCode::NO_CONTENT }))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(gateway)
}

async fn chat_completions(
    State(gateway): State<Gateway>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let class = match headers.get("x-request-class").map(HeaderValue::as_bytes) {
        None | Some(b"interactive") => RequestClass::Interactive,
        Some(b"batch") => RequestClass::Batch,
        Some(_) => {
            return (StatusCode::BAD_REQUEST, "Unknown request class").into_response();
        }
    };

    let estimate = gateway
        .profile
        .as_ref()
        .and_then(|profile| profile.estimate(&body));
    let queued_at = Instant::now();
    let Ok(admission) = gateway.scheduler.acquire(class, estimate).await else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Model queue is full").into_response();
    };

    let queue_wait = queued_at.elapsed();
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
    let queue_wait_ms = format!("{:.3}", queue_wait.as_secs_f64() * 1_000.0);
    response.headers_mut().insert(
        "x-queue-wait-ms",
        HeaderValue::from_str(&queue_wait_ms).unwrap(),
    );
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
}
