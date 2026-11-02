use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use crate::{observer::snapshot::Snapshot, upstream::UpstreamId};

type Snapshots = HashMap<UpstreamId, Snapshot>;

pub struct Window {
    baselines: VecDeque<Snapshots>,
    ticks: usize,
    tick: Duration,
}

impl Window {
    pub fn new(seed: Snapshots, ticks: usize, tick: Duration) -> Self {
        Window {
            baselines: VecDeque::from([seed]),
            ticks,
            tick,
        }
    }

    pub fn oldest(&self) -> &Snapshots {
        self.baselines
            .front()
            .expect("seeded at construction and trimmed only after a push")
    }

    pub fn ticks_held(&self) -> usize {
        self.baselines.len()
    }

    pub fn span(&self) -> Duration {
        self.tick * self.ticks_held() as u32
    }

    pub fn wanted_span(&self) -> Duration {
        self.tick * self.ticks as u32
    }

    pub fn is_full(&self) -> bool {
        self.ticks_held() >= self.ticks
    }

    pub fn advance(&mut self, current: Snapshots) {
        self.baselines.push_back(current);
        if self.baselines.len() > self.ticks {
            self.baselines.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = Duration::from_secs(15);

    #[test]
    fn the_span_grows_one_tick_per_advance_until_full() {
        let mut window = Window::new(HashMap::new(), 3, TICK);
        assert_eq!(window.span(), TICK);
        assert!(!window.is_full());

        window.advance(HashMap::new());
        window.advance(HashMap::new());
        assert_eq!(window.span(), TICK * 3);
        assert!(window.is_full());

        window.advance(HashMap::new());
        assert_eq!(window.span(), TICK * 3);
        assert_eq!(window.wanted_span(), TICK * 3);
    }
}
