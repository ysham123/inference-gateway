use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
    str::FromStr,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    Interactive,
    Batch,
}

#[derive(Clone, Copy)]
pub enum SchedulingPolicy {
    Fifo,
    Weighted,
}

impl FromStr for SchedulingPolicy {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fifo" => Ok(Self::Fifo),
            "weighted" => Ok(Self::Weighted),
            _ => Err("SCHEDULING_POLICY must be fifo or weighted"),
        }
    }
}

pub struct Scheduler {
    max_active: NonZeroUsize,
    max_waiting: usize,
    policy: SchedulingPolicy,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    active: HashSet<u64>,
    waiting: VecDeque<Waiter>,
    interactive_turns: usize,
}

struct Waiter {
    id: u64,
    class: RequestClass,
    ready: oneshot::Sender<()>,
}

#[derive(Debug)]
pub struct QueueFull;

pub struct Admission {
    scheduler: Arc<Scheduler>,
    id: u64,
}

impl Scheduler {
    pub fn new(max_active: NonZeroUsize, max_waiting: usize, policy: SchedulingPolicy) -> Self {
        Self {
            max_active,
            max_waiting,
            policy,
            state: Mutex::new(State::default()),
        }
    }

    pub async fn acquire(self: &Arc<Self>, class: RequestClass) -> Result<Admission, QueueFull> {
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
                state.waiting.push_back(Waiter {
                    id,
                    class,
                    ready: sender,
                });
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
            let Some((index, interactive_turns)) = state.next_waiter(self.policy) else {
                state.interactive_turns = 0;
                break;
            };
            let waiter = state.waiting.remove(index).unwrap();
            if waiter.ready.send(()).is_ok() {
                state.active.insert(waiter.id);
                state.interactive_turns = interactive_turns;
            }
        }
    }
}

impl State {
    fn next_waiter(&self, policy: SchedulingPolicy) -> Option<(usize, usize)> {
        if matches!(policy, SchedulingPolicy::Fifo) {
            return self.waiting.front().map(|_| (0, 0));
        }

        let first_of_class = |class| self.waiting.iter().position(|waiter| waiter.class == class);
        let interactive = first_of_class(RequestClass::Interactive);
        let batch = first_of_class(RequestClass::Batch);
        match (interactive, batch) {
            (Some(index), Some(_)) if self.interactive_turns < 3 => {
                Some((index, self.interactive_turns + 1))
            }
            (Some(_), Some(index)) => Some((index, 0)),
            (Some(index), None) | (None, Some(index)) => Some((index, 0)),
            (None, None) => None,
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
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 2, SchedulingPolicy::Fifo));
        let active = scheduler.acquire(RequestClass::Interactive).await.unwrap();

        let mut first = Box::pin(scheduler.acquire(RequestClass::Batch));
        let mut second = Box::pin(scheduler.acquire(RequestClass::Interactive));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(scheduler.acquire(RequestClass::Interactive).await.is_err());

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
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 1, SchedulingPolicy::Fifo));
        let active = scheduler.acquire(RequestClass::Interactive).await.unwrap();
        let mut queued = Box::pin(scheduler.acquire(RequestClass::Interactive));
        assert!(poll!(&mut queued).is_pending());
        drop(queued);

        let mut replacement = Box::pin(scheduler.acquire(RequestClass::Interactive));
        assert!(poll!(&mut replacement).is_pending());
        drop(active);
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn dropping_a_dispatched_request_before_it_resumes_frees_its_slot() {
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 1, SchedulingPolicy::Fifo));
        let active = scheduler.acquire(RequestClass::Interactive).await.unwrap();
        let mut queued = Box::pin(scheduler.acquire(RequestClass::Interactive));
        assert!(poll!(&mut queued).is_pending());

        drop(active);
        drop(queued);
        let mut replacement = Box::pin(scheduler.acquire(RequestClass::Interactive));
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn weighted_scheduling_gives_batch_a_turn_under_interactive_load() {
        let scheduler = Arc::new(Scheduler::new(
            NonZeroUsize::MIN,
            5,
            SchedulingPolicy::Weighted,
        ));
        let active = scheduler.acquire(RequestClass::Batch).await.unwrap();
        let mut batch = Box::pin(scheduler.acquire(RequestClass::Batch));
        assert!(poll!(&mut batch).is_pending());

        let mut interactive: Vec<_> = (0..4)
            .map(|_| Box::pin(scheduler.acquire(RequestClass::Interactive)))
            .collect();
        for request in &mut interactive {
            assert!(poll!(request).is_pending());
        }

        drop(active);
        for index in 0..3 {
            assert!(poll!(&mut batch).is_pending());
            assert!(poll!(&mut interactive[index + 1]).is_pending());
            let Poll::Ready(Ok(admitted)) = poll!(&mut interactive[index]) else {
                panic!("next interactive request should get the slot");
            };
            drop(admitted);
        }
        assert!(poll!(&mut interactive[3]).is_pending());
        assert!(matches!(poll!(&mut batch), Poll::Ready(Ok(_))));
        assert!(matches!(poll!(&mut interactive[3]), Poll::Ready(Ok(_))));
    }
}
