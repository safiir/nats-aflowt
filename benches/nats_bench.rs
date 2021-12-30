use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use std::time::{Duration, Instant};

pub fn pub_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");
    group.warm_up_time(Duration::from_secs(1));
    // TODO(dlc) - Can both be done at some time or requires custom?
    //group.throughput(Throughput::Bytes(*size as u64));
    group.throughput(Throughput::Elements(1));

    let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        let msg = &bmsg[0..*size];
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|n| async move {
                    let nc = nats::connect("127.0.0.1").await.unwrap();
                    let start = Instant::now();
                    for _i in 0..n {
                        nc.publish("bench", &msg).await.unwrap();
                    }
                    nc.flush().await.unwrap();
                    start.elapsed()
                });
        });
    }
    group.finish();
}

criterion_group!(benches, pub_benchmark);
criterion_main!(benches);
