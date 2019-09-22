use std::hash::Hash;
use std::iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::current_thread::Runtime;

use tokio_cache::AsyncCache;

async fn new_async_cache<K, V>(init: &[(K, V)]) -> AsyncCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let cache = AsyncCache::new();
    for (key, value) in init {
        cache
            .write(K::clone(key), async {
                Result::<V, ()>::Ok(V::clone(value))
            })
            .await
            .unwrap();
    }
    cache
}

fn benchmark_async_cache_reads(criterion: &mut Criterion) {
    let mut runtime = Runtime::new().unwrap();

    {
        let cache = runtime.block_on(new_async_cache::<u32, u32>(&[(1, 2)]));
        criterion.bench_function("read-uncontended-u32-u32", move |bench| {
            bench.iter(|| cache.read(&1));
        });
    }

    {
        let cache = runtime.block_on(new_async_cache::<u32, u32>(&[(1, 2)]));
        let _reader = OtherThreads::readers(1, cache.clone(), iter::repeat(1));
        criterion.bench_function("read-contended-1r-read-u32-u32", move |bench| {
            bench.iter(|| cache.read(&1));
        });
    }

    {
        let cache = runtime.block_on(new_async_cache::<u32, u32>(&[(1, 2)]));
        let _readers = OtherThreads::readers(4, cache.clone(), iter::repeat(1));
        criterion.bench_function("read-contended-4r-read-u32-u32", move |bench| {
            bench.iter(|| cache.read(&1));
        });
    }

    {
        let cache = runtime.block_on(new_async_cache::<u32, u32>(&[(1, 2)]));
        let _readers = OtherThreads::writers(4, cache.clone(), iter::repeat((1, 2)));
        criterion.bench_function("read-contended-4w-read-u32-u32", move |bench| {
            bench.iter(|| cache.read(&1));
        });
    }
}

criterion_group!(benches, benchmark_async_cache_reads);

criterion_main!(benches);

struct OtherThreads {
    flag: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl OtherThreads {
    fn readers<K, Ks, V>(n: usize, cache: AsyncCache<K, V>, mut keys: Ks) -> Self
        where
            K: Clone + Eq + Hash + Send + Sync + 'static,
            Ks: Iterator<Item = K> + Clone + Send + 'static,
            V: Clone + Send + Sync + 'static,
    {
        Self::new(n, move |_| {
            cache.read(keys.next().as_ref().unwrap());
        })
    }

    fn writers<K, Es, V>(n: usize, cache: AsyncCache<K, V>, mut entries: Es) -> Self
        where
            K: Clone + Eq + Hash + Send + Sync + 'static,
            Es: Iterator<Item = (K, V)> + Clone + Send + 'static,
            V: Clone + Send + Sync + 'static,
    {
        Self::new(n, move |runtime| {
            let (key, value) = entries.next().unwrap();
            runtime.block_on(cache.write(key, async { Result::<_, ()>::Ok(value) })).unwrap();
        })
    }

    fn new<F>(n: usize, f: F) -> Self where F: FnMut(&mut Runtime) + Send + Clone + 'static {
        let flag = Arc::new(AtomicBool::new(true));
        let handles = (0..n)
            .map(|_| {
                let flag = flag.clone();
                let mut f = f.clone();

                thread::spawn(move || {
                    let mut runtime = Runtime::new().unwrap();
                    while flag.load(Ordering::SeqCst) {
                        for _ in 0..100000 {
                            f(&mut runtime);
                        }
                    }
                })
            })
            .collect();
        OtherThreads { flag, handles }
    }
}

impl Drop for OtherThreads {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
        self.handles
            .drain(..)
            .try_for_each(JoinHandle::join)
            .unwrap();
    }
}
