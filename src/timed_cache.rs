use std::borrow::Borrow;
use std::future::Future;
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::core::{CacheCore, ExpireEntry};

struct ExpiresAt;

impl<V> ExpireEntry<ExpiringEntry<V>> for ExpiresAt {
    fn is_fresh(&self, entry: &ExpiringEntry<V>) -> bool {
        entry.is_fresh()
    }
}

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
    core: CacheCore<K, ExpiringEntry<V>>,
}

impl<K, V> TimedCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(time_to_live: Duration) -> Self {
        Self {
            time_to_live,
            core: CacheCore::new(),
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
        self.core
            .read(key, &ExpiresAt)
            .map(ExpiringEntry::into_entry)
    }

    pub async fn write<F, E>(&self, key: K, future: F) -> Result<V, E>
    where
        F: Future<Output = Result<V, E>>,
    {
        let time_to_live = self.time_to_live;
        let entry = self
            .core
            .write(
                key,
                async move {
                    let value = future.await?;
                    Ok(ExpiringEntry::fresh(value, time_to_live))
                },
                &ExpiresAt,
            )
            .await?;
        Ok(entry.into_entry())
    }
}
