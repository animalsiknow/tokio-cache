use std::borrow::Borrow;
use std::collections::hash_map::{self, HashMap};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::future::Future;
use futures::{try_ready, Async, Poll};
use state_machine_future::{RentToOwn, StateMachineFuture};
use tokio_sync::lock::Lock;
use tokio_sync::watch;

#[derive(Clone)]
pub struct CacheCore<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    entries: Arc<ArcSwap<im::HashMap<K, V>>>,
    pending_entries: Lock<HashMap<K, watch::Receiver<Option<V>>>>,
}

impl<K, V> CacheCore<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            entries: Arc::new(ArcSwap::new(Arc::new(im::HashMap::new()))),
            pending_entries: Lock::new(HashMap::new()),
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

    pub fn write<F, E>(&self, key: K, future: F, expire_entry: E) -> MachineFuture<K, F, E>
    where
        F: Future<Item = V>,
        E: ExpireEntry<V>,
    {
        let context = Context {
            key,
            core: self.clone(),
            expire_entry,
        };
        Machine::start(future, PhantomData, context)
    }
}

impl<K, V> Debug for CacheCore<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for (k, v) in &**self.entries.load() {
            debug_map.entry(&k, &v);
        }
        debug_map.finish()
    }
}

pub trait ExpireEntry<E> {
    fn is_fresh(&self, entry: &E) -> bool;
}

pub struct Context<K, F, E>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
    E: ExpireEntry<F::Item>,
{
    key: K,
    core: CacheCore<K, F::Item>,
    expire_entry: E,
}

#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
pub enum Machine<K, F, E>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
    E: ExpireEntry<F::Item>,
{
    #[state_machine_future(
        start,
        transitions(WaitingForComputation, WaitingForOtherComputation, Ready)
    )]
    WaitingForLock {
        future: F,
        not_expire_entry: PhantomData<E>,
    },

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

impl<K, F, E> PollMachine<K, F, E> for Machine<K, F, E>
where
    K: Hash + Eq + Clone,
    F: Future,
    F::Item: Clone,
    E: ExpireEntry<F::Item>,
{
    fn poll_waiting_for_lock<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingForLock<F, E>>,
        context: &'context mut RentToOwn<'context, Context<K, F, E>>,
    ) -> Poll<AfterWaitingForLock<K, F>, F::Error> {
        let mut guard = match context.core.pending_entries.poll_lock() {
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
                    let entries_lease = context.core.entries.load();
                    if let Some(value) = entries_lease.get(&context.key) {
                        if context.expire_entry.is_fresh(value) {
                            // Another task has already started and completed the computation
                            // Return the result right away.
                            return Ok(Async::Ready(
                                Ready((context.take().key, value.clone())).into(),
                            ));
                        }
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
        _context: &'context mut RentToOwn<'context, Context<K, F, E>>,
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
        context: &'context mut RentToOwn<'context, Context<K, F, E>>,
    ) -> Poll<AfterWaitingToBroadcast<K, F>, F::Error> {
        {
            let mut guard = match context.core.pending_entries.poll_lock() {
                Async::Ready(guard) => guard,
                Async::NotReady => return Ok(Async::NotReady),
            };

            let mut entries = {
                let lease = context.core.entries.load();
                im::HashMap::clone(&lease)
            };
            entries.insert(context.key.clone(), state.value.clone());
            context.core.entries.store(Arc::new(entries));

            guard.remove(&context.key);
        }

        let mut state = state.take();
        let context = context.take();
        let _ = state.channel.broadcast(Some(state.value.clone()));
        Ok(Async::Ready(Ready((context.key, state.value)).into()))
    }

    fn poll_waiting_for_other_computation<'state, 'context>(
        state: &'state mut RentToOwn<'state, WaitingForOtherComputation<F>>,
        context: &'context mut RentToOwn<'context, Context<K, F, E>>,
    ) -> Poll<AfterWaitingForOtherComputation<K, F, E>, F::Error> {
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
                not_expire_entry: PhantomData,
            }
            .into(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use futures::future::ok;
    use tokio::runtime::Runtime;

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

    #[test]
    fn fetch_an_expired_entry() {
        let mut runtime = Runtime::new().unwrap();
        let cache = CacheCore::new();

        assert_eq!(
            Ok((1, 2)),
            runtime.block_on(cache.write(1, ok::<_, ()>(2), Never))
        );
        assert_eq!(Some(2), cache.read(&1, &Never));
        assert_eq!(None, cache.read(&1, &Always));
    }

    #[test]
    fn overwrite_an_expired_entry() {
        let mut runtime = Runtime::new().unwrap();
        let cache = CacheCore::new();

        assert_eq!(
            Ok((1, 2)),
            runtime.block_on(cache.write(1, ok::<_, ()>(2), Never))
        );
        assert_eq!(
            Ok((1, 3)),
            runtime.block_on(cache.write(1, ok::<_, ()>(3), Always))
        );
        assert_eq!(Some(3), cache.read(&1, &Never));
    }
}
