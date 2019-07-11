use std::borrow::Borrow;
use std::hash::Hash;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::{Async, Poll, try_ready};

use crate::async_cache::{AsyncCache, AsyncCacheWriteFuture};

#[derive(Clone, Debug)]
struct ExpiringEntry<T> {
    entry: T,
    expires_at: Instant,
}

impl<T> ExpiringEntry<T> {
    fn fresh(entry: T, time_to_live: Duration) -> Self {
        Self {
            entry,
            expires_at: Instant::now() + time_to_live,
        }
    }

    fn into_entry(self) -> T {
        self.entry
    }

    fn is_fresh(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

#[derive(Clone, Debug)]
pub struct TimedCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    time_to_live: Duration,
    inner: AsyncCache<K, ExpiringEntry<V>>,
}

impl<K, V> TimedCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(time_to_live: Duration) -> Self {
        Self {
            time_to_live,
            inner: AsyncCache::new(),
        }
    }

    /// Reads the cached entry for the given key.
    ///
    /// Note that this will not block.
    pub fn read<BK>(&self, key: &BK) -> Option<V>
    where
        BK: Hash + Eq + ?Sized,
        K: Borrow<BK>,
    {
        self.inner
            .read(key)
            .filter(ExpiringEntry::is_fresh)
            .map(|entry| entry.clone().into_entry())
    }

    pub fn write<F>(&self, key: K, future: F) -> TimedCacheWriteFuture<K, F>
    where
        F: Future<Item = V>,
    {
        let inner = self.inner.write(
            key,
            SetupExpiryFuture {
                inner: future,
                time_to_live: self.time_to_live,
            },
        );
        TimedCacheWriteFuture { inner }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TimedCacheWriteFuture<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    inner: AsyncCacheWriteFuture<K, SetupExpiryFuture<F>>,
}

impl<K, F> Future for TimedCacheWriteFuture<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    type Item = F::Item;

    type Error = F::Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let entry = try_ready!(self.inner.poll());
        Ok(Async::Ready(entry.into_entry()))
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct SetupExpiryFuture<F>
where
    F: Future,
    F::Item: Clone,
{
    inner: F,
    time_to_live: Duration,
}

impl<F> Future for SetupExpiryFuture<F>
where
    F: Future,
    F::Item: Clone,
{
    type Item = ExpiringEntry<F::Item>;

    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let value = try_ready!(self.inner.poll());
        Ok(Async::Ready(ExpiringEntry::fresh(value, self.time_to_live)))
    }
}
