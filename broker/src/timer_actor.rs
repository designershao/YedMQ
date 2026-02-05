use crate::session::session_actor::SessionActorMessage;
use crate::session::session_actor::SessionActorMessage::{InflightRetry, KeepAliveExpired};
use actix::{Actor, AsyncContext, Context, Handler, Message, Recipient};
use std::collections::HashMap;
use std::task::Poll;
use std::time::Duration;
use tokio_util::time::delay_queue::Key;
use tokio_util::time::DelayQueue;

#[derive(Message)]
#[rtype(result = "()")]
pub struct RefreshTimer {
    pub tenant_id: String,
    pub session_id: String,
    pub timer_type: TimerType,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RemoveTimer {
    pub tenant_id: String,
    pub session_id: String,
    pub timer_type: TimerType,
}

impl Handler<RefreshTimer> for TimerActor {
    type Result = ();

    fn handle(&mut self, msg: RefreshTimer, _ctx: &mut Self::Context) -> Self::Result {
        if let Some((_, key, duration, _)) = self.sessions.get_mut(&(
            msg.tenant_id.clone(),
            msg.session_id.clone(),
            msg.timer_type,
        )) {
            self.queue.reset(key, *duration);
        }
    }
}

impl Handler<RemoveTimer> for TimerActor {
    type Result = ();

    fn handle(&mut self, msg: RemoveTimer, _ctx: &mut Self::Context) -> Self::Result {
        if let Some((_, key, _, _)) = self.sessions.remove(&(
            msg.tenant_id.clone(),
            msg.session_id.clone(),
            msg.timer_type,
        )) {
            self.queue.remove(&key);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterKeepAlive {
    pub tenant_id: String,
    pub session_id: String,
    pub keep_alive: Duration,
    pub addr: Recipient<SessionActorMessage>,
}

impl Handler<RegisterKeepAlive> for TimerActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterKeepAlive, _ctx: &mut Self::Context) -> Self::Result {
        let timeout = msg.keep_alive;
        let key = self.queue.insert(
            (
                msg.tenant_id.clone(),
                msg.session_id.clone(),
                TimerType::KeepAlive,
            ),
            timeout,
        );
        self.sessions.insert(
            (msg.tenant_id, msg.session_id, TimerType::KeepAlive),
            (msg.addr, key, timeout, TimerType::KeepAlive),
        );
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterInflight {
    pub tenant_id: String,
    pub session_id: String,
    pub inflight_retry_duration: Duration,
    pub addr: Recipient<SessionActorMessage>,
}

impl Handler<RegisterInflight> for TimerActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterInflight, _ctx: &mut Self::Context) -> Self::Result {
        let timeout = msg.inflight_retry_duration;
        let key = self.queue.insert(
            (
                msg.tenant_id.clone(),
                msg.session_id.clone(),
                TimerType::Inflight,
            ),
            timeout,
        );
        self.sessions.insert(
            (msg.tenant_id, msg.session_id, TimerType::Inflight),
            (msg.addr, key, timeout, TimerType::KeepAlive),
        );
    }
}

#[derive(Eq, PartialOrd, PartialEq, Hash, Clone, Copy, Debug)]
pub enum TimerType {
    Inflight,
    KeepAlive,
}

pub struct TimerActor {
    queue: DelayQueue<(String, String, TimerType)>, // (tenant_id, client_id, timer_type)

    sessions: HashMap<
        (String, String, TimerType),
        (Recipient<SessionActorMessage>, Key, Duration, TimerType),
    >,
}

impl Actor for TimerActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_timeout_handler(ctx);
    }
}

impl Default for TimerActor {
    fn default() -> Self {
        Self::new()
    }
}

impl TimerActor {
    pub fn new() -> Self {
        Self {
            queue: DelayQueue::new(),
            sessions: HashMap::new(),
        }
    }

    fn start_timeout_handler(&mut self, ctx: &mut Context<Self>) {
        ctx.run_interval(Duration::from_millis(10), |act, _ctx| {
            // Create a waker for polling
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);

            // Poll for expired timers
            loop {
                // Pin the queue for polling
                let mut queue_pin = std::pin::Pin::new(&mut act.queue);

                match queue_pin.as_mut().poll_expired(&mut cx) {
                    Poll::Ready(Some(expired)) => {
                        let (tenant_id, session_id, timer_type) = expired.into_inner();

                        if let Some((addr, _, _, _)) =
                            act.sessions
                                .get(&(tenant_id.clone(), session_id.clone(), timer_type))
                        {
                            match timer_type {
                                TimerType::KeepAlive => {
                                    println!(
                                        "Sending KeepAliveExpired to session {}/{}",
                                        tenant_id, session_id
                                    );
                                    addr.do_send(KeepAliveExpired);
                                }
                                TimerType::Inflight => {
                                    println!(
                                        "Sending InflightRetry to session {}/{}",
                                        tenant_id, session_id
                                    );
                                    addr.do_send(InflightRetry);
                                }
                            }
                        } else {
                            println!(
                                "Session not found: {}/{} {:?}",
                                tenant_id, session_id, timer_type
                            );
                        }
                    }
                    Poll::Ready(None) => {
                        // Queue is empty
                        break;
                    }
                    Poll::Pending => {
                        // No more expired items
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::session_actor::SessionActorMessage;
    use actix::Actor;
    use std::sync::{Arc, Mutex};
    use tokio::time::sleep;

    // Test SessionActor that tracks received messages
    #[derive(Default)]
    struct TestSessionActor {
        keep_alive_count: Arc<Mutex<usize>>,
        inflight_count: Arc<Mutex<usize>>,
    }

    impl Actor for TestSessionActor {
        type Context = Context<Self>;
    }

    impl Handler<SessionActorMessage> for TestSessionActor {
        type Result = ();
        fn handle(&mut self, _msg: SessionActorMessage, _ctx: &mut Self::Context) {
            match _msg {
                SessionActorMessage::KeepAliveExpired => {
                    let mut count = self.keep_alive_count.lock().unwrap();
                    *count += 1;
                    println!(
                        "TestSessionActor received KeepAliveTimeout, count: {}",
                        *count
                    );
                }
                SessionActorMessage::InflightRetry => {
                    let mut count = self.inflight_count.lock().unwrap();
                    *count += 1;
                }
                _ => {}
            }
        }
    }

    // ==================== 1. Basic Functionality Tests ====================

    #[actix::test]
    async fn test_register_keep_alive() {
        let timer = TimerActor::new().start();
        let keep_alive_count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: keep_alive_count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        assert_eq!(*keep_alive_count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_register_inflight() {
        let timer = TimerActor::new().start();
        let inflight_count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: Arc::new(Mutex::new(0)),
            inflight_count: inflight_count.clone(),
        }
        .start();

        timer
            .send(RegisterInflight {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                inflight_retry_duration: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        assert_eq!(*inflight_count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_register_multiple_sessions() {
        let timer = TimerActor::new().start();
        let count1 = Arc::new(Mutex::new(0));
        let count2 = Arc::new(Mutex::new(0));

        let session1 = TestSessionActor {
            keep_alive_count: count1.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        let session2 = TestSessionActor {
            keep_alive_count: count2.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session1.recipient(),
            })
            .await
            .unwrap();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant2".to_string(),
                session_id: "session2".to_string(),
                keep_alive: Duration::from_millis(150),
                addr: session2.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        assert_eq!(*count1.lock().unwrap(), 1);
        assert_eq!(*count2.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_register_overwrites_old_timer() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        // First registration 500ms
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(500),
                addr: session.clone().recipient().clone(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        // Second registration overwrites to 100ms
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        // Should trigger only once (the second registration)
        assert_eq!(*count.lock().unwrap(), 1);
    }

    // ==================== 2. Timer Expiration Tests ====================

    #[actix::test]
    async fn test_timers_expire_in_order() {
        let timer = TimerActor::new().start();
        let count1 = Arc::new(Mutex::new(0));
        let count2 = Arc::new(Mutex::new(0));
        let count3 = Arc::new(Mutex::new(0));

        let session1 = TestSessionActor {
            keep_alive_count: count1.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        let session2 = TestSessionActor {
            keep_alive_count: count2.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        let session3 = TestSessionActor {
            keep_alive_count: count3.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(300),
                addr: session1.recipient(),
            })
            .await
            .unwrap();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant2".to_string(),
                session_id: "session2".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session2.recipient(),
            })
            .await
            .unwrap();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant3".to_string(),
                session_id: "session3".to_string(),
                keep_alive: Duration::from_millis(200),
                addr: session3.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count2.lock().unwrap(), 1);
        assert_eq!(*count1.lock().unwrap(), 0);
        assert_eq!(*count3.lock().unwrap(), 0);

        sleep(Duration::from_millis(100)).await;
        assert_eq!(*count3.lock().unwrap(), 1);
        assert_eq!(*count1.lock().unwrap(), 0);

        sleep(Duration::from_millis(100)).await;
        assert_eq!(*count1.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_both_timer_types_work() {
        let timer = TimerActor::new().start();
        let keep_alive_count = Arc::new(Mutex::new(0));
        let inflight_count = Arc::new(Mutex::new(0));

        let session = TestSessionActor {
            keep_alive_count: keep_alive_count.clone(),
            inflight_count: inflight_count.clone(),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.clone().recipient().clone(),
            })
            .await
            .unwrap();

        timer
            .send(RegisterInflight {
                tenant_id: "tenant1".to_string(),
                session_id: "session2".to_string(),
                inflight_retry_duration: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        assert_eq!(*keep_alive_count.lock().unwrap(), 1);
        assert_eq!(*inflight_count.lock().unwrap(), 1);
    }

    // ==================== 3. Refresh Timer Tests ====================

    #[actix::test]
    async fn test_refresh_timer_extends_timeout() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(200),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;

        // Refresh the timer
        timer
            .send(RefreshTimer {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                timer_type: TimerType::KeepAlive,
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        // Without refresh, it should have triggered by now
        assert_eq!(*count.lock().unwrap(), 0);

        sleep(Duration::from_millis(100)).await;
        // After refresh, it should trigger now
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_refresh_multiple_times() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        for _ in 0..5 {
            sleep(Duration::from_millis(50)).await;
            timer
                .send(RefreshTimer {
                    tenant_id: "tenant1".to_string(),
                    session_id: "session1".to_string(),
                    timer_type: TimerType::KeepAlive,
                })
                .await
                .unwrap();
        }

        assert_eq!(*count.lock().unwrap(), 0);
        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_refresh_nonexistent_timer() {
        let timer = TimerActor::new().start();

        // Refresh non-existent timer, should not panic
        timer
            .send(RefreshTimer {
                tenant_id: "nonexistent".to_string(),
                session_id: "nonexistent".to_string(),
                timer_type: TimerType::KeepAlive,
            })
            .await
            .unwrap();
    }

    #[actix::test]
    async fn test_refresh_wrong_timer_type() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        // Refresh with wrong timer type
        timer
            .send(RefreshTimer {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                timer_type: TimerType::Inflight,
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        // Should trigger normally because refresh didn't work
        assert_eq!(*count.lock().unwrap(), 1);
    }

    // ==================== 4. Remove Timer Tests ====================

    #[actix::test]
    async fn test_remove_timer_prevents_timeout() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        timer
            .send(RemoveTimer {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                timer_type: TimerType::KeepAlive,
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;
        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[actix::test]
    async fn test_remove_nonexistent_timer() {
        let timer = TimerActor::new().start();

        // Remove non-existent timer, should not panic
        timer
            .send(RemoveTimer {
                tenant_id: "nonexistent".to_string(),
                session_id: "nonexistent".to_string(),
                timer_type: TimerType::KeepAlive,
            })
            .await
            .unwrap();
    }

    #[actix::test]
    async fn test_remove_then_register_again() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        // First registration
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.clone().recipient().clone(),
            })
            .await
            .unwrap();

        // Remove
        timer
            .send(RemoveTimer {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                timer_type: TimerType::KeepAlive,
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count.lock().unwrap(), 0);

        // Re-register
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count.lock().unwrap(), 1);
    }

    // ==================== 5. Boundary Condition Tests ====================

    #[actix::test]
    async fn test_very_short_timeout() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(1),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_zero_timeout() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::ZERO,
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[actix::test]
    async fn test_many_timers() {
        let timer = TimerActor::new().start();
        let mut counts = Vec::new();

        for i in 0..100 {
            let count = Arc::new(Mutex::new(0));
            counts.push(count.clone());

            let session = TestSessionActor {
                keep_alive_count: count,
                inflight_count: Arc::new(Mutex::new(0)),
            }
            .start();

            timer
                .send(RegisterKeepAlive {
                    tenant_id: format!("tenant{}", i),
                    session_id: format!("session{}", i),
                    keep_alive: Duration::from_millis(100),
                    addr: session.recipient(),
                })
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(200)).await;

        for count in counts {
            assert_eq!(*count.lock().unwrap(), 1);
        }
    }

    // ==================== 6. Concurrency Tests ====================

    #[actix::test]
    async fn test_concurrent_register() {
        let timer = TimerActor::new().start();
        let mut handles = vec![];

        for i in 0..10 {
            let timer_clone = timer.clone();
            let handle = actix::spawn(async move {
                let session = TestSessionActor {
                    keep_alive_count: Arc::new(Mutex::new(0)),
                    inflight_count: Arc::new(Mutex::new(0)),
                }
                .start();

                timer_clone
                    .send(RegisterKeepAlive {
                        tenant_id: format!("tenant{}", i),
                        session_id: format!("session{}", i),
                        keep_alive: Duration::from_millis(200),
                        addr: session.recipient(),
                    })
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        sleep(Duration::from_millis(300)).await;
    }

    #[actix::test]
    async fn test_concurrent_refresh() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(500),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let timer_clone = timer.clone();
            let handle = tokio::spawn(async move {
                timer_clone
                    .send(RefreshTimer {
                        tenant_id: "tenant1".to_string(),
                        session_id: "session1".to_string(),
                        timer_type: TimerType::KeepAlive,
                    })
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        sleep(Duration::from_millis(600)).await;
        assert_eq!(*count.lock().unwrap(), 1);
    }

    // ==================== 7. Complete Workflow Tests ====================

    #[actix::test]
    async fn test_complete_session_lifecycle() {
        let timer = TimerActor::new().start();
        let count = Arc::new(Mutex::new(0));
        let session = TestSessionActor {
            keep_alive_count: count.clone(),
            inflight_count: Arc::new(Mutex::new(0)),
        }
        .start();

        // Register
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.clone().recipient(),
            })
            .await
            .unwrap();

        // Refresh multiple times
        for _ in 0..3 {
            sleep(Duration::from_millis(50)).await;
            timer
                .send(RefreshTimer {
                    tenant_id: "tenant1".to_string(),
                    session_id: "session1".to_string(),
                    timer_type: TimerType::KeepAlive,
                })
                .await
                .unwrap();
        }

        // Stop refreshing and let it timeout
        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count.lock().unwrap(), 1);

        // Re-register
        timer
            .send(RegisterKeepAlive {
                tenant_id: "tenant1".to_string(),
                session_id: "session1".to_string(),
                keep_alive: Duration::from_millis(100),
                addr: session.recipient(),
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(150)).await;
        assert_eq!(*count.lock().unwrap(), 2);
    }
}
