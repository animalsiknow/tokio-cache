use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::hash::Hash;

use futures::future::Future;
use futures::{try_ready, Async, Poll};

use crate::core::{CacheCore, ExpireEntry, MachineFuture};

struct Never;

impl<V> ExpireEntry<V> for Never {
    fn is_fresh(&self, _: &V) -> bool {
        true
    }
}

/// A cache where reads are wait-free and where writes are asynchronous, such as it can be used
/// within code that cannot block.
#[derive(Clone)]
pub struct AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    core: CacheCore<K, V>,
}

impl<K, V> AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        AsyncCache {
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
        self.core.read(key, &Never)
    }

    /// Writes the result of the given `future` in the cache at entry `key`.
    ///
    /// If multiple writes, concurrent or not, on the same key are issued, only one of the futures
    /// will be polled and the others will be dropped.
    pub fn write<F>(&self, key: K, future: F) -> AsyncCacheWriteFuture<K, F>
    where
        F: Future<Item = V>,
    {
        let inner = self.core.write(key, future, Never);
        AsyncCacheWriteFuture { inner }
    }
}

impl<K, V> Debug for AsyncCache<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AsyncCache")
            .field("entries", &self.core)
            .finish()
    }
}

/// A future that will write a cache entry when polled to completion.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AsyncCacheWriteFuture<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    inner: MachineFuture<K, F, Never>,
}

impl<K, F> Future for AsyncCacheWriteFuture<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    type Item = F::Item;

    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (_, value) = try_ready!(self.inner.poll());
        Ok(Async::Ready(value))
    }
}

#[cfg(test)]
mod tests {
    use futures::future::ok;
    use tokio::runtime::Runtime;

    use super::AsyncCache;

    #[test]
    fn insert_then_get_single_entry() {
        let mut runtime = Runtime::new().unwrap();
        let cache = AsyncCache::new();

        assert_eq!(None, cache.read(&1));
        assert_eq!(Ok(2), runtime.block_on(cache.write(1, ok::<_, ()>(2))));
        assert_eq!(Some(2), cache.read(&1));
    }

    #[test]
    fn writing_twice_does_not_override() {
        let mut runtime = Runtime::new().unwrap();
        let cache = AsyncCache::new();

        assert_eq!(None, cache.read(&1));
        assert_eq!(Ok(2), runtime.block_on(cache.write(1, ok::<_, ()>(2))));
        assert_eq!(Some(2), cache.read(&1));
        assert_eq!(Ok(2), runtime.block_on(cache.write(1, ok::<_, ()>(3))));
        assert_eq!(Some(2), cache.read(&1));
    }
}
