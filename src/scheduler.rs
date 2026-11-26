use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

pub struct Scheduler {
    max_active: NonZeroUsize,
    max_waiting: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    active: HashSet<u64>,
    waiting: VecDeque<Waiter>,
}

struct Waiter {
    id: u64,
    ready: oneshot::Sender<()>,
}

#[derive(Debug)]
pub struct QueueFull;

pub struct Admission {
    scheduler: Arc<Scheduler>,
    id: u64,
}

impl Scheduler {
    pub fn new(max_active: NonZeroUsize, max_waiting: usize) -> Self {
        Self {
            max_active,
            max_waiting,
            state: Mutex::new(State::default()),
        }
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<Admission, QueueFull> {
        let (id, ready) = {
            let mut state = self.state.lock().unwrap();
            let at_capacity = state.active.len() >= self.max_active.get();
            if at_capacity && state.waiting.len() >= self.max_waiting {
                return Err(QueueFull);
            }

            let id = state.next_id;
            state.next_id += 1;
            if at_capacity {
                let (sender, receiver) = oneshot::channel();
                state.waiting.push_back(Waiter { id, ready: sender });
                (id, Some(receiver))
            } else {
                state.active.insert(id);
                (id, None)
            }
        };

        let admission = Admission {
            scheduler: Arc::clone(self),
            id,
        };
        if let Some(ready) = ready {
            ready.await.expect("a queued request retains its scheduler");
        }
        Ok(admission)
    }

    fn release(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        if !state.active.remove(&id) {
            state.waiting.retain(|waiter| waiter.id != id);
            return;
        }

        while state.active.len() < self.max_active.get() {
            let Some(waiter) = state.waiting.pop_front() else {
                break;
            };
            if waiter.ready.send(()).is_ok() {
                state.active.insert(waiter.id);
            }
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.scheduler.release(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::poll;
    use std::task::Poll;

    #[tokio::test]
    async fn queued_requests_start_in_order_and_excess_requests_fail() {
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 2));
        let active = scheduler.acquire().await.unwrap();

        let mut first = Box::pin(scheduler.acquire());
        let mut second = Box::pin(scheduler.acquire());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(scheduler.acquire().await.is_err());

        drop(active);
        assert!(poll!(&mut second).is_pending());
        let Poll::Ready(Ok(first)) = poll!(&mut first) else {
            panic!("first queued request should get the slot");
        };

        drop(first);
        assert!(matches!(poll!(&mut second), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn dropping_a_queued_request_frees_queue_space() {
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 1));
        let active = scheduler.acquire().await.unwrap();
        let mut queued = Box::pin(scheduler.acquire());
        assert!(poll!(&mut queued).is_pending());
        drop(queued);

        let mut replacement = Box::pin(scheduler.acquire());
        assert!(poll!(&mut replacement).is_pending());
        drop(active);
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn dropping_a_dispatched_request_before_it_resumes_frees_its_slot() {
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 1));
        let active = scheduler.acquire().await.unwrap();
        let mut queued = Box::pin(scheduler.acquire());
        assert!(poll!(&mut queued).is_pending());

        drop(active);
        drop(queued);
        let mut replacement = Box::pin(scheduler.acquire());
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }
}
