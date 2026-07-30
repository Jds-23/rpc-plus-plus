use std::sync::atomic::{AtomicUsize, Ordering};

pub trait Decider {
    type Item;

    fn decide(&self) -> Option<&Self::Item>;
}

pub struct RoundRobin<T> {
    items: Vec<T>,
    next: AtomicUsize,
} 

impl<T> RoundRobin<T> {
    pub fn new(items: impl IntoIterator<Item = T>) -> Self {
        Self {
            items: items.into_iter().collect(),
            next: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<T> Decider for RoundRobin<T> {
    type Item = T;

    fn decide(&self) -> Option<&T> {
        if self.items.is_empty() {
            return None;
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        Some(&self.items[i % self.items.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycles_and_wraps() {
        let rr = RoundRobin::new(["a", "b", "c"]);
        let got: Vec<_> = (0..5).filter_map(|_| rr.decide()).copied().collect();
        assert_eq!(got, ["a", "b", "c", "a", "b"]);
    }

    #[test]
    fn empty_yields_none() {
        let rr: RoundRobin<u8> = RoundRobin::new([]);
        assert!(rr.decide().is_none());
    }
}
