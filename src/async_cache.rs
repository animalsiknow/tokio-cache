use std::borrow::Borrow;
use std::collections::hash_map::{self, HashMap};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::future::Future;
use futures::{try_ready, Async, Poll};
use state_machine_future::{RentToOwn, StateMachineFuture};
use tokio_sync::lock::Lock;
use tokio_sync::watch;

/// A cache where reads are wait-free and where writes are asynchronous, such as it can be used
/// within code that cannot block.
#[derive(Clone)]
pub struct AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    entries: Arc<ArcSwap<im::HashMap<K, V>>>,
    pending_entries: Lock<HashMap<K, watch::Receiver<Option<V>>>>,
}

impl<K, V> AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        AsyncCache {
            entries: Arc::new(ArcSwap::new(Arc::new(im::HashMap::new()))),
            pending_entries: Lock::new(HashMap::new()),
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
        let lease = self.entries.lease();
        im::HashMap::get(&lease, key).map(V::clone)
    }

    /// Writes the result of the given `future` in the cache at entry `key`.
    ///
    /// If multiple writes, concurrent or not, on the same key are issued, only one of the futures
    /// will be polled and the others will be dropped.
    pub fn write<F>(&self, key: K, future: F) -> AsyncCacheWriteFuture<K, F>
    where
        F: Future<Item = V>,
    {
        let context = Context {
            key,
            entries: Arc::clone(&self.entries),
            pending_entries: self.pending_entries.clone(),
        };
        let inner = Machine::start(future, context);
        AsyncCacheWriteFuture { inner }
    }
}

impl<K, V> Debug for AsyncCache<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for (k, v) in &*self.entries.load() {
            debug_map.entry(&k, &v);
        }
        debug_map.finish()
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
    inner: MachineFuture<K, F>,
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

struct Context<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    key: K,
    entries: Arc<ArcSwap<im::HashMap<K, F::Item>>>,
    pending_entries: Lock<HashMap<K, watch::Receiver<Option<F::Item>>>>,
}

#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
enum Machine<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    #[state_machine_future(
        start,
        transitions(WaitingForComputation, WaitingForOtherComputation, Ready)
    )]
    WaitingForLock { future: F },

    #[state_machine_future(transitions(WaitingToBroadcast))]
    WaitingForComputation {
        channel: watch::Sender<Option<F::Item>>,
        future: F,
    },

    #[state_machine_future(transitions(Ready))]
    WaitingToBroadcast {
        channel: watch::Sender<Option<F::Item>>,
        value: F::Item,
    },

    #[state_machine_future(transitions(WaitingForLock, Ready))]
    WaitingForOtherComputation {
        channel: watch::Receiver<Option<F::Item>>,
        future: F,
    },

    #[state_machine_future(error)]
    Failed(F::Error),

    #[state_machine_future(ready)]
    Ready((K, F::Item)),
}

impl<K, F> PollMachine<K, F> for Machine<K, F>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
{
    fn poll_waiting_for_lock<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingForLock<F>>,
        context: &'context mut RentToOwn<'context, Context<K, F>>,
    ) -> Poll<AfterWaitingForLock<K, F>, F::Error> {
        let mut guard = match context.pending_entries.poll_lock() {
            Async::Ready(guard) => guard,
            Async::NotReady => return Ok(Async::NotReady),
        };

        match guard.entry(context.key.clone()) {
            hash_map::Entry::Occupied(occupied) => {
                // Another task has already started the computation, wait on the watch for the entry
                // to be ready.
                let channel = occupied.get().clone();
                Ok(Async::Ready(
                    WaitingForOtherComputation {
                        future: state.take().future,
                        channel,
                    }
                    .into(),
                ))
            }

            hash_map::Entry::Vacant(vacant) => {
                {
                    let entries_lease = context.entries.lease();
                    if let Some(value) = entries_lease.get(&context.key) {
                        // Another task has already started and completed the computation. Return
                        // the result right away.
                        return Ok(Async::Ready(
                            Ready((context.take().key, value.clone())).into(),
                        ));
                    }
                }

                // This task "won" the race to populate the entry. Setup a watch on which other
                // tasks can wait.
                let (sender, receiver) = watch::channel(None);
                vacant.insert(receiver);

                Ok(Async::Ready(
                    WaitingForComputation {
                        channel: sender,
                        future: state.take().future,
                    }
                    .into(),
                ))
            }
        }
    }

    fn poll_waiting_for_computation<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingForComputation<F>>,
        _context: &'context mut RentToOwn<'context, Context<K, F>>,
    ) -> Poll<AfterWaitingForComputation<F>, F::Error> {
        let value = try_ready!(state.future.poll());

        Ok(Async::Ready(
            WaitingToBroadcast {
                channel: state.take().channel,
                value,
            }
            .into(),
        ))
    }

    fn poll_waiting_to_broadcast<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingToBroadcast<F>>,
        context: &'context mut RentToOwn<'context, Context<K, F>>,
    ) -> Poll<AfterWaitingToBroadcast<K, F>, F::Error> {
        {
            let mut guard = match context.pending_entries.poll_lock() {
                Async::Ready(guard) => guard,
                Async::NotReady => return Ok(Async::NotReady),
            };

            let mut entries = {
                let lease = context.entries.lease();
                im::HashMap::clone(&lease)
            };
            entries.insert(context.key.clone(), state.value.clone());
            context.entries.store(Arc::new(entries));

            guard.remove(&context.key);
        }

        let mut state = state.take();
        let context = context.take();
        let _ = state.channel.broadcast(Some(state.value.clone()));
        Ok(Async::Ready(Ready((context.key, state.value)).into()))
    }

    fn poll_waiting_for_other_computation<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingForOtherComputation<F>>,
        context: &'context mut RentToOwn<'context, Context<K, F>>,
    ) -> Poll<AfterWaitingForOtherComputation<K, F>, F::Error> {
        loop {
            match state.channel.poll_ref() {
                Err(_) => unreachable!(),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    break;
                }
                Ok(Async::Ready(Some(r#ref))) => {
                    if let Some(value) = &*r#ref {
                        return Ok(Async::Ready(
                            Ready((context.take().key, value.clone())).into(),
                        ));
                    }
                }
            }
        }

        // The process that was populating the entry has failed. Start from the beginning.
        return Ok(Async::Ready(
            WaitingForLock {
                future: state.take().future,
            }
            .into(),
        ));
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
