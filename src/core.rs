use std::borrow::Borrow;
use std::collections::hash_map::{self, HashMap};
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio_sync::{watch, Mutex};

pub struct CacheCore<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    entries: ArcSwap<im::HashMap<K, V>>,
    pending_entries: Mutex<HashMap<K, watch::Receiver<Option<V>>>>,
}

impl<K, V> CacheCore<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            entries: ArcSwap::new(Arc::new(im::HashMap::new())),
            pending_entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn read<BK, E>(&self, key: &BK, expire_entry: &E) -> Option<V>
    where
        BK: Hash + Eq + ?Sized,
        K: Borrow<BK>,
        E: ExpireEntry<V>,
    {
        let lease = self.entries.load();
        im::HashMap::get(&lease, key)
            .filter(|&entry| expire_entry.is_fresh(entry))
            .map(V::clone)
    }

    pub async fn write<F, X, E>(&self, key: K, future: F, expire_entry: &X) -> Result<V, E>
    where
        F: Future<Output = Result<V, E>>,
        X: ExpireEntry<V>,
    {
        loop {
            let mut guard = self.pending_entries.lock().await;

            match guard.entry(key.clone()) {
                hash_map::Entry::Occupied(occupied) => {
                    // Another task has already started the computation, wait on the watch for the
                    // entry to be ready.
                    if let Some(v) = self.wait_on_other_computation(occupied.get().clone()).await {
                        // The task on which we were waiting succeeded.
                        return Ok(v);
                    }
                }

                hash_map::Entry::Vacant(vacant) => {
                    {
                        let entries_lease = self.entries.load();
                        if let Some(value) = entries_lease.get(&key) {
                            if expire_entry.is_fresh(value) {
                                // Another task has already started and completed the computation
                                // Return the result right away.
                                return Ok(V::clone(value));
                            }
                        }
                    }

                    // This task "won" the race to populate the entry. Setup a watch on which other
                    // tasks can wait.
                    let (sender, receiver) = watch::channel(None);
                    vacant.insert(receiver);
                    mem::drop(guard);

                    let v = future.await?;
                    self.update_store(sender, key, v.clone())
                        .await;
                    return Ok(v);
                }
            }
        }
    }

    async fn wait_on_other_computation(&self, mut channel: watch::Receiver<Option<V>>) -> Option<V>
    where
        V: Clone,
    {
        loop {
            match channel.recv().await {
                Some(Some(value)) => return Some(value),
                Some(None) => (),
                None => return None,
            }
        }
    }

    async fn update_store(
        &self,
        sender: watch::Sender<Option<V>>,
        key: K,
        value: V,
    ) {
        let mut guard = self.pending_entries.lock().await;
        guard.remove(&key);

        let mut entries = {
            let entries = self.entries.load();
            im::HashMap::clone(&entries)
        };
        entries.insert(key, value.clone());
        self.entries.store(Arc::new(entries));

        let _ = sender.broadcast(Some(value));
    }
}

impl<K, V> Debug for CacheCore<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for (key, value) in &**self.entries.load() {
            debug_map.entry(&key, &value);
        }
        debug_map.finish()
    }
}

pub trait ExpireEntry<E> {
    fn is_fresh(&self, entry: &E) -> bool;
}

#[cfg(test)]
mod tests {
    use super::{CacheCore, ExpireEntry};

    struct Never;

    impl<V> ExpireEntry<V> for Never {
        fn is_fresh(&self, _: &V) -> bool {
            true
        }
    }

    struct Always;

    impl<V> ExpireEntry<V> for Always {
        fn is_fresh(&self, _: &V) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn fetch_an_expired_entry() {
        let cache = CacheCore::new();

        assert_eq!(
            Ok(2),
            cache
                .write(1, async { Result::<_, ()>::Ok(2) }, &Never)
                .await,
        );
        assert_eq!(Some(2), cache.read(&1, &Never));
        assert_eq!(None, cache.read(&1, &Always));
    }

    #[tokio::test]
    async fn overwrite_an_expired_entry() {
        let cache = CacheCore::<i32, i32>::new();

        assert_eq!(
            Ok(2),
            cache
                .write(1, async { Result::<_, ()>::Ok(2) }, &Never)
                .await,
        );
        assert_eq!(
            Ok(3),
            cache
                .write(1, async { Result::<_, ()>::Ok(3) }, &Always)
                .await,
        );
        assert_eq!(Some(3), cache.read(&1, &Never));
    }
}
