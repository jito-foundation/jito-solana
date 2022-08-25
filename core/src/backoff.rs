// BackoffStrategy currently implements a simple
// Fibonacci backoff strategy with hardcoded values.
// Currently the only use case is for retrying long lived
// connection loops in recv_verify_stage, as use cases
// expand more strategies will be added.

use std::cmp::min;

const INITIAL_LAST_WAIT: u64 = 0;
const INITIAL_CUR_WAIT: u64 = 100;
const MAX_WAIT: u64 = 1000;

#[derive(Copy, Clone)]
pub struct BackoffStrategy {
    // Wait times in ms
    last_wait: u64,
    cur_wait: u64,
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffStrategy {
    pub fn new() -> BackoffStrategy {
        BackoffStrategy {
            last_wait: INITIAL_LAST_WAIT,
            cur_wait: INITIAL_CUR_WAIT,
        }
    }

    pub fn next_wait(&mut self) -> u64 {
        let next_wait = min(self.cur_wait + self.last_wait, MAX_WAIT);
        self.last_wait = self.cur_wait;
        self.cur_wait = next_wait;
        next_wait
    }

    pub fn reset(&mut self) {
        self.last_wait = INITIAL_LAST_WAIT;
        self.cur_wait = INITIAL_CUR_WAIT;
    }
}
