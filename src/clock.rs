use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

pub trait Clock: Send + Sync {
    fn now_utc(&self) -> DateTime<Utc>;
    fn now_instant(&self) -> Instant;
    fn now_unix_nanos(&self) -> u128;
}

#[derive(Clone)]
pub struct SharedClock(Arc<dyn Clock>);

impl SharedClock {
    pub fn real() -> Self {
        Self(Arc::new(RealClock))
    }

    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self(clock)
    }

    pub fn now_utc(&self) -> DateTime<Utc> {
        self.0.now_utc()
    }

    pub fn now_instant(&self) -> Instant {
        self.0.now_instant()
    }

    pub fn now_unix_nanos(&self) -> u128 {
        self.0.now_unix_nanos()
    }
}

struct RealClock;

impl Clock for RealClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_unix_nanos(&self) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    }
}

#[derive(Clone)]
pub struct FakeClock {
    base_instant: Instant,
    state: Arc<Mutex<FakeClockState>>,
}

struct FakeClockState {
    now_utc: DateTime<Utc>,
    monotonic_offset: Duration,
}

impl FakeClock {
    pub fn new(now_utc: DateTime<Utc>) -> Self {
        Self {
            base_instant: Instant::now(),
            state: Arc::new(Mutex::new(FakeClockState {
                now_utc,
                monotonic_offset: Duration::ZERO,
            })),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().unwrap();
        state.now_utc += chrono::Duration::from_std(duration).unwrap();
        state.monotonic_offset += duration;
    }

    pub fn shared(&self) -> SharedClock {
        SharedClock::new(Arc::new(self.clone()))
    }
}

impl Clock for FakeClock {
    fn now_utc(&self) -> DateTime<Utc> {
        self.state.lock().unwrap().now_utc
    }

    fn now_instant(&self) -> Instant {
        let state = self.state.lock().unwrap();
        self.base_instant + state.monotonic_offset
    }

    fn now_unix_nanos(&self) -> u128 {
        self.now_utc()
            .timestamp_nanos_opt()
            .map(|value| value.max(0) as u128)
            .unwrap_or(0)
    }
}
