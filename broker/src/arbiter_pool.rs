use actix::prelude::*;
use log::info;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Arbiter Pool - Manages a fixed number of worker threads.
pub struct ArbiterPool {
    arbiters: Vec<Arbiter>,
    next_index: AtomicUsize,
}

impl ArbiterPool {
    /// Creates an Arbiter pool of a specified size.
    pub fn new(name: &str, size: usize) -> Arc<Self> {
        let arbiters: Vec<_> = (0..size).map(|_| Arbiter::new()).collect();

        info!("arbiter pool '{}' created with {} arbiters", name, size);

        Arc::new(Self {
            arbiters,
            next_index: AtomicUsize::new(0),
        })
    }

    /// Creates a pool based on the number of CPU cores.
    pub fn with_cpu_factor(name: &str, factor: usize) -> Arc<Self> {
        let size = num_cpus::get() * factor;
        Self::new(name, size)
    }

    /// Gets the next Arbiter in a round-robin fashion.
    pub fn next(&self) -> &Arbiter {
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        &self.arbiters[idx % self.arbiters.len()]
    }

    /// Gets a specific Arbiter by hashing the key.
    pub fn get_by_key(&self, key: &str) -> &Arbiter {
        let hash = self.hash_key(key);
        &self.arbiters[hash % self.arbiters.len()]
    }

    /// Gets the size of the pool.
    pub fn size(&self) -> usize {
        self.arbiters.len()
    }

    /// Starts an actor in the pool (round-robin).
    pub fn start_actor<A, F>(&self, factory: F) -> Addr<A>
    where
        A: Actor<Context = Context<A>>,
        F: FnOnce() -> A + Send + 'static,
    {
        let arbiter = self.next();
        A::start_in_arbiter(&arbiter.handle(), move |_| factory())
    }

    /// Starts an actor in the pool (based on key hash).
    pub fn start_actor_with_key<A, F>(&self, key: &str, factory: F) -> Addr<A>
    where
        A: Actor<Context = Context<A>>,
        F: FnOnce() -> A + Send + 'static,
    {
        let arbiter = self.get_by_key(key);
        A::start_in_arbiter(&arbiter.handle(), move |_| factory())
    }

    fn hash_key(&self, key: &str) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}
