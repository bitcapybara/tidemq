use serde::{Deserialize, Serialize};

/// Two bounded second buckets retain a conservative recent demand without storing messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Traffic {
    second: u64,
    current: u64,
    previous: u64,
}

impl Traffic {
    pub fn new(now_ms: u64) -> Self {
        Self {
            second: now_ms / 1_000,
            current: 0,
            previous: 0,
        }
    }

    fn advance(&mut self, now_ms: u64) {
        let second = (now_ms / 1_000).max(self.second);
        if second > self.second {
            self.previous = if second.saturating_sub(self.second) == 1 {
                self.current
            } else {
                0
            };
            self.current = 0;
            self.second = second;
        }
    }

    pub fn record(&mut self, bytes: u64, now_ms: u64) {
        self.advance(now_ms);
        self.current = self.current.saturating_add(bytes);
    }

    pub fn recent(mut self, now_ms: u64) -> u64 {
        self.advance(now_ms);
        self.current.saturating_add(self.previous)
    }

    pub fn valid(self, last_activity_ms: u64) -> bool {
        self.second <= last_activity_ms / 1_000
    }
}
