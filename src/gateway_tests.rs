use super::*;
use std::convert::Infallible;
use tokio::sync::Notify;

async fn serve(app: Router) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Url::parse(&format!("http://{address}/v1/chat/completions")).unwrap()
}

struct DisconnectSignal(Arc<Notify>);

impl Drop for DisconnectSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn a_disconnected_stream_releases_the_slot_and_closes_upstream() {
    let signal = Arc::new(Notify::new());
    let backend = Router::new()
        .route(
            "/v1/chat/completions",
            post(
                |State(signal): State<Arc<Notify>>, body: Bytes| async move {
                    if body.as_ref() != b"hold" {
                        return Body::from("data: [DONE]\n\n");
                    }

                    let disconnect = DisconnectSignal(signal);
                    let first = stream::once(async { Ok::<_, Infallible>("data: first\n\n") });
                    let rest = stream::poll_fn(move |_| {
                        let _keep_alive = &disconnect;
                        std::task::Poll::Pending
                    });
                    Body::from_stream(first.chain(rest))
                },
            ),
        )
        .with_state(Arc::clone(&signal));
    let backend_url = serve(backend).await;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let gateway_url = serve(router(Gateway {
        client: client.clone(),
        completions_url: backend_url,
        profile: None,
        scheduler: Arc::new(Scheduler::new(NonZeroUsize::MIN, 0, SchedulingPolicy::Fifo)),
    }))
    .await;

    let mut active = client
        .post(gateway_url.clone())
        .body("hold")
        .send()
        .await
        .unwrap();
    assert_eq!(active.status(), StatusCode::OK);
    assert_eq!(active.chunk().await.unwrap().unwrap(), "data: first\n\n");

    let excess = client.post(gateway_url.clone()).send().await.unwrap();
    assert_eq!(excess.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(excess.text().await.unwrap(), "Model queue is full");

    drop(active);
    tokio::time::timeout(Duration::from_secs(5), signal.notified())
        .await
        .expect("the upstream stream should close");
    let next = client.post(gateway_url).send().await.unwrap();
    assert_eq!(next.status(), StatusCode::OK);
    assert_eq!(next.text().await.unwrap(), "data: [DONE]\n\n");
}
