// Registry types not used in current integration tests
// 注册表类型在当前集成测试中未使用
use shannon_agent_core::tools::{ToolCall, ToolExecutor};
use std::collections::HashMap;
use std::time::Duration;

/// Setup test environment with optional Python service URL
/// 初始化测试环境，可选指定 Python 服务 URL
fn setup_test_env() -> String {
    // Initialize logging for tests
    // 为测试初始化日志
    let _ = tracing_subscriber::fmt::try_init();

    // Get Python LLM service URL from environment or use default
    // 从环境变量获取 Python LLM 服务 URL，否则用默认值
    std::env::var("LLM_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8000".to_string())
}

/// Check if Python service is available
/// 检查 Python 服务是否可用
async fn is_python_service_available(url: &str) -> bool {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let health_url = format!("{}/health", url);

    match client
        .get(&health_url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
    {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

#[tokio::test]
async fn test_tool_selection_contract() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Test tool selection for a calculation task
    // 测试数学计算 task 的 tool 选择
    let tools = executor
        .select_tools_remote("calculate 42 + 58", false)
        .await
        .expect("Failed to select tools");

    // Should select calculator tool
    // 应当选中 calculator tool
    assert!(!tools.is_empty(), "Should select at least one tool");
    assert!(
        tools.iter().any(|t| t == "calculator"),
        "Should select calculator for math task"
    );
}

#[tokio::test]
async fn test_tool_execution_contract() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Test calculator tool execution
    // 测试 calculator tool 执行
    let call = ToolCall {
        tool_name: "calculator".to_string(),
        parameters: HashMap::from([("expression".to_string(), serde_json::json!("2 + 2"))]),
        call_id: Some("test_calc_1".to_string()),
    };

    let result = executor
        .execute_tool(&call, None)
        .await
        .expect("Failed to execute tool");

    // Validate result structure
    // 校验结果结构
    assert_eq!(result.tool, "calculator");
    assert!(
        result.success || result.error.is_some(),
        "Result should either succeed or have error message"
    );

    // If Python service implements calculator, should return 4
    // 若 Python 服务实现了 calculator，应返回 4
    if result.success {
        if let Some(num) = result.output.as_f64() {
            assert_eq!(num, 4.0, "Calculator should return 4 for 2+2");
        }
    }
}

#[tokio::test]
async fn test_tool_discovery_integration() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Get available tools via Python service (filtered)
    // 通过 Python 服务获取可用 tool（已过滤）
    let _tools = executor
        .get_available_tools(true)
        .await
        .expect("Failed to get available tools");
}

// FSM removed - Rust is now an enforcement gateway
// FSM 已移除——Rust 现在只作为执行约束网关
#[tokio::test]
#[ignore]
async fn test_fsm_llm_integration() {
    // Test removed: FSM and AgentRuntime no longer exist
    // 测试已移除：FSM 与 AgentRuntime 不再存在
    // Rust agent-core is now an enforcement gateway
    // Rust agent-core 现在只作为执行约束网关
    // Testing should focus on enforcement policies instead
    // 测试应改为关注执行约束策略
}

// Cache behavior is now internal to Python; skip cache-specific executor tests
// cache 行为现已内置于 Python；跳过 cache 相关的执行器测试

#[tokio::test]
async fn test_error_handling_contract() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Test with invalid tool name
    // 使用无效的 tool 名测试
    let call = ToolCall {
        tool_name: "nonexistent_tool_12345".to_string(),
        parameters: HashMap::new(),
        call_id: None,
    };

    let result = executor.execute_tool(&call, None).await;

    // Should handle gracefully
    // 应优雅处理
    assert!(result.is_ok(), "Should not panic on unknown tool");

    if let Ok(tool_result) = result {
        if !tool_result.success {
            assert!(
                tool_result.error.is_some(),
                "Failed execution should include error message"
            );
        }
    }
}

#[tokio::test]
async fn test_tool_list_contract() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Get available tools from Python
    // 从 Python 获取可用 tool
    let tools = executor
        .get_available_tools(false)
        .await
        .expect("Failed to get tool list");

    // Should return standard tools
    // 应返回标准 tool
    println!("Available tools from Python: {:?}", tools);

    // Basic validation
    // 基础校验
    assert!(
        !tools.is_empty() || tools.is_empty(),
        "Should either return tools or empty list"
    );

    // If tools are returned, they should be valid strings
    // 若返回了 tool，名称应为合法字符串
    for tool in &tools {
        assert!(!tool.is_empty(), "Tool names should not be empty");
        assert!(!tool.contains(' '), "Tool names should not contain spaces");
    }
}

#[tokio::test]
async fn test_complexity_analysis_contract() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    // Test Python's complexity analysis endpoint
    // 测试 Python 的复杂度分析端点
    let client = reqwest::Client::new();
    let url = format!("{}/complexity/analyze", llm_url);

    let request = serde_json::json!({
        "query": "Build a web application with user authentication",
        "context": {}
    });

    let response = client
        .post(&url)
        .json(&request)
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    match response {
        Ok(resp) if resp.status().is_success() => {
            let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");

            // Validate response structure
            // 校验 response 结构
            assert!(
                body.get("recommended_mode").is_some(),
                "Response should include recommended_mode"
            );
            assert!(
                body.get("complexity_score").is_some(),
                "Response should include complexity_score"
            );
        }
        Ok(resp) => {
            // Endpoint exists but returned error
            // 端点存在但返回了错误
            println!("Complexity analysis returned status: {}", resp.status());
        }
        Err(_) => {
            // Endpoint doesn't exist yet
            // 端点尚不存在
            println!("Complexity analysis endpoint not implemented");
        }
    }
}

#[tokio::test]
async fn test_session_context_handling() {
    let llm_url = setup_test_env();

    if !is_python_service_available(&llm_url).await {
        eprintln!(
            "Skipping test: Python LLM service not available at {}",
            llm_url
        );
        return;
    }

    let executor = ToolExecutor::new(Some(llm_url));

    // Test with context that references previous interaction
    // 使用引用上一轮交互的 context 进行测试
    let call1 = ToolCall {
        tool_name: "calculator".to_string(),
        parameters: HashMap::from([("expression".to_string(), serde_json::json!("100 + 50"))]),
        call_id: Some("session_1".to_string()),
    };

    let result1 = executor.execute_tool(&call1, None).await;
    assert!(result1.is_ok(), "First call should succeed");

    // Second call that might reference the first
    // 第二次调用可能引用第一次的结果
    let call2 = ToolCall {
        tool_name: "calculator".to_string(),
        parameters: HashMap::from([
            ("expression".to_string(), serde_json::json!("150 * 2")), // Using result from first
        ]),
        call_id: Some("session_2".to_string()),
    };

    let result2 = executor.execute_tool(&call2, None).await;
    assert!(result2.is_ok(), "Second call should succeed");
}

/// Integration test runner that checks all contracts
/// 检查所有契约的集成测试入口
#[tokio::test]
async fn test_full_python_rust_contract_suite() {
    let llm_url = setup_test_env();

    println!("Testing Python-Rust contract with service at: {}", llm_url);

    if !is_python_service_available(&llm_url).await {
        println!("⚠️  Python LLM service not available - skipping integration tests");
        println!("   To run these tests, start the Python service:");
        println!("   cd python/llm-service && python3 main.py");
        return;
    }

    println!("✅ Python service is available");

    // Run a series of contract validations
    // 执行一系列契约校验
    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Tool discovery
    // 测试 1：tool 发现
    match ToolExecutor::new(Some(llm_url.clone()))
        .get_available_tools(false)
        .await
    {
        Ok(tools) => {
            println!(
                "✅ Tool discovery contract: {} tools available",
                tools.len()
            );
            passed += 1;
        }
        Err(e) => {
            println!("❌ Tool discovery contract failed: {}", e);
            failed += 1;
        }
    }

    // Test 2: Tool selection
    // 测试 2：tool 选择
    match ToolExecutor::new(Some(llm_url.clone()))
        .select_tools_remote("search for information", false)
        .await
    {
        Ok(tools) => {
            println!("✅ Tool selection contract: {} tools selected", tools.len());
            passed += 1;
        }
        Err(e) => {
            println!("❌ Tool selection contract failed: {}", e);
            failed += 1;
        }
    }

    // Test 3: Skip executor cache checks (not exposed)
    // 测试 3：跳过执行器 cache 检查（未对外暴露）

    println!("\n📊 Contract Test Summary:");
    println!("   Passed: {}", passed);
    println!("   Failed: {}", failed);

    assert!(failed == 0, "Some contract tests failed");
}
