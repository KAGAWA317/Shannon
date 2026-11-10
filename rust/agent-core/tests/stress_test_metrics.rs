// Stress test for OnceLock-based metrics initialization
// 针对基于 OnceLock 的指标初始化的压力测试
// Tests for race conditions and concurrent initialization
// 测试竞态条件与并发初始化

use shannon_agent_core::metrics;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrent_metrics_initialization() {
    // Test that multiple threads can safely initialize metrics
    // 验证多个线程可以安全地初始化指标
    let init_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..10 {
        let init_count = init_count.clone();
        let error_count = error_count.clone();

        let handle = thread::spawn(move || {
            // Each thread tries to initialize metrics
            // 每个线程都尝试初始化指标
            match metrics::init_metrics() {
                Ok(_) => {
                    init_count.fetch_add(1, Ordering::SeqCst);
                    println!("Thread {} initialized metrics (idempotent OK)", i);
                }
                Err(e) => {
                    // No error is expected in idempotent init
                    // 幂等初始化不应产生错误
                    error_count.fetch_add(1, Ordering::SeqCst);
                    eprintln!("Thread {} unexpected init error: {}", i, e);
                }
            }

            // Try to use metrics after initialization
            // 初始化后尝试使用指标
            let timer = metrics::TaskTimer::new("test");
            thread::sleep(Duration::from_millis(10));
            timer.complete("success", Some(100));
        });

        handles.push(handle);
    }

    // Wait for all threads
    // 等待所有线程结束
    for handle in handles {
        handle.join().unwrap();
    }

    // All threads should see Ok(()) due to idempotent initialization
    // 由于初始化是幂等的，所有线程都应看到 Ok(())
    assert_eq!(
        init_count.load(Ordering::SeqCst),
        10,
        "All threads should observe successful initialization"
    );
    assert_eq!(
        error_count.load(Ordering::SeqCst),
        0,
        "No errors should occur during initialization"
    );

    // Verify metrics are accessible
    // 验证指标可被访问
    let metrics_output = metrics::get_metrics();
    assert!(
        !metrics_output.is_empty(),
        "Metrics should be available after initialization"
    );
}

#[test]
fn test_metrics_usage_without_initialization() {
    // Test that metrics can be safely accessed even if not initialized
    // 验证即使未初始化，指标也能被安全访问
    // This simulates a scenario where metrics initialization fails
    // 模拟指标初始化失败的场景

    // Try to use a timer without initialization
    // 在未初始化的情况下尝试使用计时器
    let timer = metrics::TaskTimer::new("test_mode");
    timer.complete("success", Some(42));

    // This should not panic, just silently skip recording
    // 这不应 panic，只是静默跳过记录
    let metrics_output = metrics::get_metrics();
    // Output might be empty or contain some metrics depending on test order
    // 输出可能为空，也可能含部分指标，取决于测试执行顺序
    println!("Metrics output length: {}", metrics_output.len());
}

#[test]
fn test_memory_pool_concurrent_access() {
    use bytes::Bytes;
    use shannon_agent_core::memory::MemoryPool;

    // Create a shared memory pool
    // 创建共享内存池
    let pool = Arc::new(MemoryPool::new(10)); // 10MB pool

    let mut handles = vec![];

    // Spawn multiple threads that allocate and deallocate memory
    // 启动多个线程分配与释放内存
    for i in 0..20 {
        let pool = pool.clone();

        let handle = thread::spawn(move || {
            // Use tokio runtime for async operations
            // 使用 tokio 运行时执行异步操作
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                let key = format!("thread_{}", i);
                let data = Bytes::from(vec![i as u8; 100_000]); // 100KB per thread

                // Try to allocate
                // 尝试分配
                match pool.allocate(key.clone(), data.clone(), 60).await {
                    Ok(_) => {
                        println!("Thread {} allocated 100KB", i);

                        // Retrieve the data
                        // 读取数据
                        if let Some(retrieved) = pool.retrieve(&key).await {
                            assert_eq!(retrieved, data, "Retrieved data should match");
                        }

                        // Deallocate
                        // 释放
                        let _ = pool.deallocate(&key).await;
                        println!("Thread {} deallocated", i);
                    }
                    Err(e) => {
                        // Expected for some threads due to memory limit
                        // 部分线程因内存上限而失败，属预期
                        println!("Thread {} allocation failed (expected): {}", i, e);
                    }
                }
            });
        });

        handles.push(handle);
    }

    // Wait for all threads
    // 等待所有线程结束
    for handle in handles {
        handle.join().unwrap();
    }

    // Check final pool state
    // 检查内存池最终状态
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (used, max) = pool.get_usage_stats().await;
        println!("Final pool state: {}/{} bytes used", used, max);
        assert!(used <= max, "Used memory should not exceed max");
    });
}

#[test]
fn test_oncelock_double_initialization() {
    // Test that OnceLock properly prevents double initialization
    // 验证 OnceLock 能正确防止重复初始化
    use std::sync::OnceLock;

    static TEST_METRIC: OnceLock<String> = OnceLock::new();

    // First initialization should succeed
    // 首次初始化应成功
    let result1 = TEST_METRIC.set("first".to_string());
    assert!(result1.is_ok(), "First set should succeed");

    // Second initialization should fail
    // 第二次初始化应失败
    let result2 = TEST_METRIC.set("second".to_string());
    assert!(result2.is_err(), "Second set should fail");

    // Value should be the first one
    // 值应保持为第一次设置的内容
    assert_eq!(TEST_METRIC.get().unwrap(), "first");
}
