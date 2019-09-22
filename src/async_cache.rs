use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use crate::core::{CacheCore, ExpireEntry};

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
    core: Arc<CacheCore<K, V>>,
}

impl<K, V> AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        AsyncCache {
            core: Arc::new(CacheCore::new()),
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
    pub async fn write<F, E>(&self, key: K, future: F) -> Result<V, E>
    where
        F: Future<Output = Result<V, E>>,
    {
        if let Some(value) = self.read(&key) {
            return Ok(value)
        }

        self.core.write(key, future, &Never).await
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

impl<K, V> Default for AsyncCache<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncCache;

    #[tokio::test]
    async fn insert_then_get_single_entry() {
        let cache = AsyncCache::new();

        assert_eq!(None, cache.read(&1));
        assert_eq!(
            Ok(2),
            cache.write(1, async { Result::<_, ()>::Ok(2) }).await
        );
        assert_eq!(Some(2), cache.read(&1));
    }

    #[tokio::test]
    async fn writing_twice_does_not_override() {
        let cache = AsyncCache::new();

        assert_eq!(None, cache.read(&1));
        assert_eq!(
            Ok(2),
            cache.write(1, async { Result::<_, ()>::Ok(2) }).await
        );
        assert_eq!(Some(2), cache.read(&1));
        assert_eq!(
            Ok(2),
            cache.write(1, async { Result::<_, ()>::Ok(3) }).await
        );
        assert_eq!(Some(2), cache.read(&1));
    }
}
