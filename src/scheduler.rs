use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
    Reserved,
    Adaptive { slack: Duration },
}

impl FromStr for SchedulingPolicy {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fifo" => Ok(Self::Fifo),
            "weighted" => Ok(Self::Weighted),
            "reserved" => Ok(Self::Reserved),
            _ => Err("SCHEDULING_POLICY must be fifo, weighted, reserved, or adaptive"),
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
    active: HashMap<u64, Active>,
    waiting: VecDeque<Waiter>,
    interactive_turns: usize,
}

struct Active {
    class: RequestClass,
    expected_finish: Option<Instant>,
}

impl Active {
    fn new(class: RequestClass, estimate: Option<Duration>) -> Self {
        Self {
            class,
            expected_finish: estimate.and_then(|duration| Instant::now().checked_add(duration)),
        }
    }
}

struct Waiter {
    id: u64,
    class: RequestClass,
    ready: oneshot::Sender<()>,
    estimate: Option<Duration>,
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

    pub async fn acquire(
        self: &Arc<Self>,
        class: RequestClass,
        estimate: Option<Duration>,
    ) -> Result<Admission, QueueFull> {
        let (id, ready) = {
            let mut state = self.state.lock().unwrap();
            if class == RequestClass::Batch
                && matches!(self.policy, SchedulingPolicy::Adaptive { .. })
            {
                self.dispatch(&mut state);
            }
            let older_batch_waiting = class == RequestClass::Batch
                && matches!(self.policy, SchedulingPolicy::Adaptive { .. })
                && state.waiting.iter().any(|waiter| waiter.class == class);
            let can_start = !older_batch_waiting
                && state.can_start(class, estimate, self.policy, self.max_active);
            if !can_start && state.waiting.len() >= self.max_waiting {
                return Err(QueueFull);
            }

            let id = state.next_id;
            state.next_id += 1;
            if !can_start {
                let (sender, receiver) = oneshot::channel();
                state.waiting.push_back(Waiter {
                    id,
                    class,
                    ready: sender,
                    estimate,
                });
                (id, Some(receiver))
            } else {
                state.active.insert(id, Active::new(class, estimate));
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
        if state.active.remove(&id).is_none() {
            state.waiting.retain(|waiter| waiter.id != id);
        }
        self.dispatch(&mut state);
    }

    fn dispatch(&self, state: &mut State) {
        while state.active.len() < self.max_active.get() {
            let Some((index, interactive_turns)) = state.next_waiter(self.policy, self.max_active)
            else {
                state.interactive_turns = 0;
                break;
            };
            let waiter = state.waiting.remove(index).unwrap();
            if waiter.ready.send(()).is_ok() {
                state
                    .active
                    .insert(waiter.id, Active::new(waiter.class, waiter.estimate));
                state.interactive_turns = interactive_turns;
            }
        }
    }
}

impl State {
    fn can_start(
        &self,
        class: RequestClass,
        estimate: Option<Duration>,
        policy: SchedulingPolicy,
        max_active: NonZeroUsize,
    ) -> bool {
        if self.active.len() >= max_active.get() {
            return false;
        }
        if class == RequestClass::Batch {
            if let SchedulingPolicy::Adaptive { slack } = policy {
                if self.waiting.iter().any(|waiter| {
                    waiter.class == RequestClass::Interactive && !waiter.ready.is_closed()
                }) {
                    return false;
                }
                if self.active.len() == max_active.get() - 1 {
                    let Some(estimate) = estimate else {
                        return false;
                    };
                    let now = Instant::now();
                    let mut earliest = estimate;
                    for active in self.active.values() {
                        let Some(remaining) = active
                            .expected_finish
                            .and_then(|finish| finish.checked_duration_since(now))
                            .filter(|remaining| !remaining.is_zero())
                        else {
                            return false;
                        };
                        earliest = earliest.min(remaining);
                    }
                    return earliest <= slack;
                }
            } else if matches!(policy, SchedulingPolicy::Reserved) {
                let active_batch = self
                    .active
                    .values()
                    .filter(|active| active.class == RequestClass::Batch)
                    .count();
                return active_batch < max_active.get() - 1;
            }
        }
        true
    }

    fn next_waiter(
        &self,
        policy: SchedulingPolicy,
        max_active: NonZeroUsize,
    ) -> Option<(usize, usize)> {
        if matches!(policy, SchedulingPolicy::Fifo) {
            return self.waiting.front().map(|_| (0, 0));
        }

        let first_of_class = |class| self.waiting.iter().position(|waiter| waiter.class == class);
        let interactive = first_of_class(RequestClass::Interactive);
        let batch = first_of_class(RequestClass::Batch).filter(|&index| {
            self.can_start(
                RequestClass::Batch,
                self.waiting[index].estimate,
                policy,
                max_active,
            )
        });
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
        let active = scheduler
            .acquire(RequestClass::Interactive, None)
            .await
            .unwrap();

        let mut first = Box::pin(scheduler.acquire(RequestClass::Batch, None));
        let mut second = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(
            scheduler
                .acquire(RequestClass::Interactive, None)
                .await
                .is_err()
        );

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
        let active = scheduler
            .acquire(RequestClass::Interactive, None)
            .await
            .unwrap();
        let mut queued = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        assert!(poll!(&mut queued).is_pending());
        drop(queued);

        let mut replacement = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        assert!(poll!(&mut replacement).is_pending());
        drop(active);
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn dropping_a_dispatched_request_before_it_resumes_frees_its_slot() {
        let scheduler = Arc::new(Scheduler::new(NonZeroUsize::MIN, 1, SchedulingPolicy::Fifo));
        let active = scheduler
            .acquire(RequestClass::Interactive, None)
            .await
            .unwrap();
        let mut queued = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        assert!(poll!(&mut queued).is_pending());

        drop(active);
        drop(queued);
        let mut replacement = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        assert!(matches!(poll!(&mut replacement), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn weighted_scheduling_gives_batch_a_turn_under_interactive_load() {
        let scheduler = Arc::new(Scheduler::new(
            NonZeroUsize::MIN,
            5,
            SchedulingPolicy::Weighted,
        ));
        let active = scheduler.acquire(RequestClass::Batch, None).await.unwrap();
        let mut batch = Box::pin(scheduler.acquire(RequestClass::Batch, None));
        assert!(poll!(&mut batch).is_pending());

        let mut interactive: Vec<_> = (0..4)
            .map(|_| Box::pin(scheduler.acquire(RequestClass::Interactive, None)))
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

    #[tokio::test]
    async fn reserved_capacity_accepts_chat_even_when_the_batch_queue_is_full() {
        let scheduler = Arc::new(Scheduler::new(
            NonZeroUsize::new(2).unwrap(),
            1,
            SchedulingPolicy::Reserved,
        ));
        let first_batch = scheduler.acquire(RequestClass::Batch, None).await.unwrap();
        let mut second_batch = Box::pin(scheduler.acquire(RequestClass::Batch, None));
        assert!(poll!(&mut second_batch).is_pending());
        assert!(scheduler.acquire(RequestClass::Batch, None).await.is_err());

        let mut chat = Box::pin(scheduler.acquire(RequestClass::Interactive, None));
        let Poll::Ready(Ok(chat)) = poll!(&mut chat) else {
            panic!("chat should use the reserved slot");
        };
        drop(chat);
        assert!(poll!(&mut second_batch).is_pending());

        drop(first_batch);
        assert!(matches!(poll!(&mut second_batch), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn adaptive_borrows_headroom_with_known_short_work() {
        let scheduler = Arc::new(Scheduler::new(
            NonZeroUsize::new(2).unwrap(),
            0,
            SchedulingPolicy::Adaptive {
                slack: Duration::from_secs(1),
            },
        ));
        let _long = scheduler
            .acquire(RequestClass::Batch, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert!(
            scheduler
                .acquire(RequestClass::Batch, Some(Duration::from_secs(5)))
                .await
                .is_err()
        );
        assert!(
            scheduler
                .acquire(RequestClass::Batch, Some(Duration::from_millis(500)))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn adaptive_keeps_headroom_when_an_active_estimate_has_expired() {
        let scheduler = Arc::new(Scheduler::new(
            NonZeroUsize::new(2).unwrap(),
            0,
            SchedulingPolicy::Adaptive {
                slack: Duration::from_secs(1),
            },
        ));
        let _overdue = scheduler
            .acquire(RequestClass::Batch, Some(Duration::ZERO))
            .await
            .unwrap();
        assert!(
            scheduler
                .acquire(RequestClass::Batch, Some(Duration::from_millis(100)))
                .await
                .is_err()
        );
        assert!(
            scheduler
                .acquire(RequestClass::Interactive, None)
                .await
                .is_ok()
        );
    }
}
