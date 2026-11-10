#![cfg(feature = "wasi")]
#![cfg(feature = "wasi")]
#[cfg(test)]
mod tests {
    use shannon_agent_core::sandbox::WasmSandbox;
    use std::path::Path;
    use std::time::Instant;

    #[tokio::test]
    #[ignore] // Ignore by default since it requires Python WASM file
    async fn test_wasm_module_caching_performance() {
        // This test demonstrates the performance improvement from caching
        // 本测试演示 cache 带来的性能提升
        let sandbox = WasmSandbox::new().expect("Failed to create sandbox");

        // Path to Python WASM (adjust if needed)
        // Python WASM 的路径（如有不同请调整）
        let python_wasm = Path::new("/opt/wasm-interpreters/python-3.11.4.wasm");

        if !python_wasm.exists() {
            eprintln!("Python WASM not found at {:?}, skipping test", python_wasm);
            return;
        }

        // First execution - will compile and cache
        // 首次执行——会编译并写入 cache
        let start1 = Instant::now();
        let result1 = sandbox
            .execute_wasm(python_wasm, "print('Hello from cached Python!')")
            .await;
        let duration1 = start1.elapsed();

        assert!(result1.is_ok(), "First execution failed: {:?}", result1);
        println!("First execution (compile + cache): {:?}", duration1);

        // Second execution - should use cached module
        // 第二次执行——应复用 cache 的模块
        let start2 = Instant::now();
        let result2 = sandbox
            .execute_wasm(python_wasm, "print('Hello again from cached Python!')")
            .await;
        let duration2 = start2.elapsed();

        assert!(result2.is_ok(), "Second execution failed: {:?}", result2);
        println!("Second execution (from cache): {:?}", duration2);

        // Third execution - verify cache still works
        // 第三次执行——验证 cache 仍然有效
        let start3 = Instant::now();
        let result3 = sandbox
            .execute_wasm(python_wasm, "print('Third time from cache!')")
            .await;
        let duration3 = start3.elapsed();

        assert!(result3.is_ok(), "Third execution failed: {:?}", result3);
        println!("Third execution (from cache): {:?}", duration3);

        // Cache should make subsequent executions significantly faster
        // cache 应让后续执行显著加快
        // Typically, compilation takes 100-500ms, cached execution takes 1-10ms
        // 通常编译需 100-500ms，命中 cache 的执行需 1-10ms
        println!("\nPerformance improvement:");
        println!("First execution: {:?}", duration1);
        println!("Average cached: {:?}", (duration2 + duration3) / 2);

        // The cached executions should be at least 5x faster
        // 命中 cache 的执行至少应快 5 倍
        let speedup = duration1.as_millis() as f64 / duration2.as_millis().max(1) as f64;
        println!("Speedup factor: {:.2}x", speedup);

        // Clear cache for cleanup
        // 清理 cache
        WasmSandbox::clear_module_cache().await;
    }

    #[tokio::test]
    async fn test_module_cache_clear() {
        // Test that cache can be cleared
        // 验证 cache 可以被清空
        WasmSandbox::clear_module_cache().await;
        // If this doesn't panic, the clear operation works
        // 若不 panic，说明清空操作正常
    }
}
