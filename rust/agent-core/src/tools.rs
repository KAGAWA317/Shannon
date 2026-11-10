#[cfg(feature = "wasi")]
use crate::wasi_sandbox::WasiSandbox;
use crate::{
    firecracker_client::{FirecrackerExecuteRequest, FirecrackerExecutorClient},
    workspace::WorkspaceManager,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use base64::Engine;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub tool_name: String,
    pub parameters: HashMap<String, serde_json::Value>,
    pub call_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same minimal wasm as in wasi_sandbox tests
    // 与 wasi_sandbox 测试相同的最小 wasm
    const MINIMAL_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00, 0x03,
        0x02, 0x01, 0x00, 0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00,
        0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b,
    ];

    #[tokio::test]
    #[cfg(feature = "wasi")]
    async fn test_code_executor_with_base64_payload() {
        let wasi = WasiSandbox::new().expect("sandbox");
        let exec = ToolExecutor::new_with_wasi(Some(wasi), None);

        let b64 = base64::engine::general_purpose::STANDARD.encode(MINIMAL_WASM);
        let mut params = HashMap::new();
        params.insert("wasm_base64".to_string(), serde_json::Value::String(b64));

        let call = ToolCall {
            tool_name: "code_executor".to_string(),
            parameters: params,
            call_id: None,
        };
        let res = exec.execute_tool(&call, None).await.expect("tool result");
        assert!(res.success, "expected success: {:?}", res.error);
        assert_eq!(res.output, serde_json::Value::String(String::new()));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub tool: String,
    pub success: bool,
    pub output: serde_json::Value,
    pub error: Option<String>,
}

pub struct ToolExecutor {
    llm_service_url: String,
    #[cfg(feature = "wasi")]
    wasi: Option<WasiSandbox>,
    /// When true, Firecracker errors fail fast without WASI fallback.
    /// 为 true 时，Firecracker 错误直接失败，不回退到 WASI。
    /// Set via DISABLE_WASI_FALLBACK=1 env var (for EKS where Firecracker is required).
    /// 通过环境变量 DISABLE_WASI_FALLBACK=1 设置（EKS 环境要求必须使用 Firecracker）。
    disable_wasi_fallback: bool,
}

impl ToolExecutor {
    /// Check if WASI fallback should be disabled (for EKS Firecracker-only mode)
    /// 判断是否应禁用 WASI 回退（用于 EKS 仅 Firecracker 模式）
    fn should_disable_wasi_fallback() -> bool {
        std::env::var("DISABLE_WASI_FALLBACK")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    pub fn new(llm_service_url: Option<String>) -> Self {
        Self {
            llm_service_url: llm_service_url
                .or_else(|| std::env::var("LLM_SERVICE_URL").ok())
                .unwrap_or_else(|| "http://llm-service:8000".to_string()),
            #[cfg(feature = "wasi")]
            wasi: None,
            disable_wasi_fallback: Self::should_disable_wasi_fallback(),
        }
    }

    #[cfg(feature = "wasi")]
    pub fn new_with_wasi(wasi: Option<WasiSandbox>, llm_service_url: Option<String>) -> Self {
        Self {
            llm_service_url: llm_service_url
                .or_else(|| std::env::var("LLM_SERVICE_URL").ok())
                .unwrap_or_else(|| "http://llm-service:8000".to_string()),
            wasi,
            disable_wasi_fallback: Self::should_disable_wasi_fallback(),
        }
    }

    #[cfg(not(feature = "wasi"))]
    pub fn new_with_wasi(_wasi: Option<()>, llm_service_url: Option<String>) -> Self {
        Self {
            llm_service_url: llm_service_url
                .or_else(|| std::env::var("LLM_SERVICE_URL").ok())
                .unwrap_or_else(|| "http://llm-service:8000".to_string()),
            disable_wasi_fallback: Self::should_disable_wasi_fallback(),
        }
    }

    #[cfg(feature = "wasi")]
    pub fn set_wasi(&mut self, wasi: Option<WasiSandbox>) {
        self.wasi = wasi;
    }

    #[cfg(not(feature = "wasi"))]
    pub fn set_wasi(&mut self, _wasi: Option<()>) {
        // No-op when WASI is disabled
        // 未启用 WASI 时为空操作
    }

    /// Select tools remotely (stub implementation)
    /// 远程选择 tool（桩实现）
    pub async fn select_tools_remote(
        &self,
        _task: &str,
        _exclude_dangerous: bool,
    ) -> Result<Vec<String>> {
        // Stub implementation - return basic tools for math calculations
        // 桩实现：仅返回用于数学计算的基础 tool
        Ok(vec!["calculator".to_string()])
    }

    /// Execute a tool via the LLM service
    /// 通过 LLM 服务执行 tool
    pub async fn execute_tool(
        &self,
        tool_call: &ToolCall,
        session_context: Option<&prost_types::Struct>,
    ) -> Result<ToolResult> {
        info!(
            "Executing tool: {} with parameters: {:?}",
            tool_call.tool_name, tool_call.parameters
        );

        if self.should_route_to_firecracker(tool_call) {
            return self.execute_firecracker(tool_call, session_context).await;
        }

        // Route calculator to local execution
        // calculator 走本地执行
        if tool_call.tool_name == "calculator" {
            if let Some(expression) = tool_call
                .parameters
                .get("expression")
                .and_then(|v| v.as_str())
            {
                info!(
                    "Executing calculator locally with expression: {}",
                    expression
                );

                // Convert Python-style ** to meval's ^ for exponentiation
                // 将 Python 风格的 ** 转为 meval 的 ^ 以表示幂运算
                let converted_expression = expression.replace("**", "^");
                info!("Converted expression for meval: {}", converted_expression);

                // Use meval for mathematical expression evaluation
                // 使用 meval 求值数学表达式
                match meval::eval_str(&converted_expression) {
                    Ok(result) => {
                        // Check for infinity or NaN which indicate math errors
                        // 检查无穷大或 NaN，这类结果表示数学错误
                        if result.is_infinite() || result.is_nan() {
                            let error_msg = if result.is_infinite() {
                                "Math error: division by zero"
                            } else {
                                "Math error: invalid operation"
                            };
                            warn!("{}", error_msg);
                            return Ok(ToolResult {
                                tool: tool_call.tool_name.clone(),
                                success: false,
                                output: serde_json::Value::Null,
                                error: Some(error_msg.to_string()),
                            });
                        }

                        info!("Calculator result: {}", result);
                        return Ok(ToolResult {
                            tool: tool_call.tool_name.clone(),
                            success: true,
                            output: serde_json::json!({
                                "result": result,
                                "expression": expression
                            }),
                            error: None,
                        });
                    }
                    Err(e) => {
                        warn!("Calculator evaluation error: {}", e);
                        return Ok(ToolResult {
                            tool: tool_call.tool_name.clone(),
                            success: false,
                            output: serde_json::Value::Null,
                            error: Some(format!("Math evaluation error: {}", e)),
                        });
                    }
                }
            } else {
                return Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some("Missing 'expression' parameter for calculator".to_string()),
                });
            }
        }

        // Route code execution to WASI sandbox when requested
        // request 时代码执行路由到 WASI sandbox
        #[cfg(feature = "wasi")]
        if tool_call.tool_name == "code_executor" {
            if let Some(wasi) = &self.wasi {
                // Expect a wasm module path and optional stdin
                // 期望 wasm 模块路径及可选 stdin
                let stdin = tool_call
                    .parameters
                    .get("stdin")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Extract argv parameters if provided (needed for Python WASM)
                // 若提供 argv 参数则提取（Python WASM 需要）
                let argv = tool_call
                    .parameters
                    .get("argv")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect::<Vec<String>>()
                    });

                debug!(
                    "code_executor: stdin length={}, argv={:?}",
                    stdin.len(),
                    argv
                );

                // Prefer base64 payload if provided
                // 若提供 base64 载荷则优先使用
                let wasm_bytes_res: Result<Vec<u8>> = if let Some(b64) = tool_call
                    .parameters
                    .get("wasm_base64")
                    .and_then(|v| v.as_str())
                {
                    base64::engine::general_purpose::STANDARD
                        .decode(b64.trim())
                        .context("Failed to decode wasm_base64 payload")
                } else if let Some(path_val) = tool_call
                    .parameters
                    .get("wasm_path")
                    .and_then(|v| v.as_str())
                {
                    fs::read(path_val)
                        .await
                        .with_context(|| format!("Failed to read wasm module at {}", path_val))
                } else {
                    Err(anyhow::anyhow!(
                        "missing 'wasm_base64' or 'wasm_path' parameter"
                    ))
                };

                match wasm_bytes_res {
                    Ok(bytes) => match wasi.execute_wasm_with_args(&bytes, stdin, argv).await {
                        Ok(output) => {
                            return Ok(ToolResult {
                                tool: tool_call.tool_name.clone(),
                                success: true,
                                output: serde_json::Value::String(output),
                                error: None,
                            });
                        }
                        Err(e) => {
                            let msg = format!("WASI execution error: {}", e);
                            warn!("{}", msg);
                            return Ok(ToolResult {
                                tool: tool_call.tool_name.clone(),
                                success: false,
                                output: serde_json::Value::Null,
                                error: Some(msg),
                            });
                        }
                    },
                    Err(e) => {
                        warn!("code_executor parameter error: {}", e);
                        return Ok(ToolResult {
                            tool: tool_call.tool_name.clone(),
                            success: false,
                            output: serde_json::Value::Null,
                            error: Some(e.to_string()),
                        });
                    }
                }
            } else {
                warn!("WASI sandbox not configured; falling back to HTTP tool execution");
            }
        }

        let client = reqwest::Client::new();
        let url = format!("{}/tools/execute", self.llm_service_url);

        // Convert session_context from prost Struct to JSON if available
        // 若存在 session_context，则从 prost Struct 转为 JSON
        let context_json = session_context.map(|ctx| {
            let mut map = serde_json::Map::new();
            for (k, v) in &ctx.fields {
                map.insert(k.clone(), crate::grpc_server::prost_value_to_json(v));
            }
            serde_json::Value::Object(map)
        });

        let mut request_body = serde_json::json!({
            "tool_name": tool_call.tool_name,
            "parameters": tool_call.parameters,
        });

        if let Some(ctx) = context_json {
            request_body["session_context"] = ctx;
        }

        let response = client.post(&url).json(&request_body).send().await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            warn!("Tool execution failed: {}", error_text);
            return Ok(ToolResult {
                tool: tool_call.tool_name.clone(),
                success: false,
                output: serde_json::Value::Null,
                error: Some(error_text),
            });
        }

        let result: serde_json::Value = response.json().await?;

        Ok(ToolResult {
            tool: tool_call.tool_name.clone(),
            success: result["success"].as_bool().unwrap_or(false),
            output: result["output"].clone(),
            error: result["error"].as_str().map(String::from),
        })
    }

    /// Get available tools from the LLM service
    /// 从 LLM 服务获取可用 tool
    pub async fn get_available_tools(&self, exclude_dangerous: bool) -> Result<Vec<String>> {
        debug!("Fetching available tools");

        let client = reqwest::Client::new();
        let url = format!(
            "{}/tools/list?exclude_dangerous={}",
            self.llm_service_url, exclude_dangerous
        );

        let response = client.get(&url).send().await?;

        if !response.status().is_success() {
            warn!("Failed to fetch available tools");
            return Ok(vec![]);
        }

        let tools: Vec<String> = response.json().await?;
        debug!("Available tools: {:?}", tools);

        Ok(tools)
    }

    fn should_route_to_firecracker(&self, tool_call: &ToolCall) -> bool {
        // Route firecracker_executor tool directly
        // firecracker_executor tool 直接路由
        if tool_call.tool_name == "firecracker_executor" {
            info!("Routing to Firecracker: tool_name is firecracker_executor");
            return true;
        }

        // Only route code_executor to Firecracker
        // 仅 code_executor 会路由到 Firecracker
        if tool_call.tool_name != "code_executor" {
            return false;
        }

        // Check PYTHON_EXECUTOR_MODE env var directly for reliability
        // 直接读取 PYTHON_EXECUTOR_MODE 环境变量，更可靠
        let mode = std::env::var("PYTHON_EXECUTOR_MODE")
            .map(|v| v.to_lowercase())
            .unwrap_or_else(|_| "wasi".to_string());

        let has_code =
            tool_call.parameters.contains_key("code") || tool_call.parameters.contains_key("stdin");

        let should_route = mode == "firecracker" && has_code;
        info!(
            "Firecracker routing: mode={}, has_code_or_stdin={}, route={}",
            mode, has_code, should_route
        );
        should_route
    }

    async fn execute_firecracker(
        &self,
        tool_call: &ToolCall,
        session_context: Option<&prost_types::Struct>,
    ) -> Result<ToolResult> {
        let client = FirecrackerExecutorClient::from_env();

        // Check if Firecracker is available - no fallback, fail fast
        // 检查 Firecracker 是否可用——不做回退，直接失败
        if !client.is_available().await {
            error!("Firecracker executor unavailable at {}", client.base_url());
            return Ok(ToolResult {
                tool: tool_call.tool_name.clone(),
                success: false,
                output: serde_json::Value::Null,
                error: Some("Python executor (Firecracker) is unavailable".to_string()),
            });
        }

        // Accept 'code' or 'stdin' parameter (LLM may use either)
        // 接受 'code' 或 'stdin' 参数（LLM 可能用其中任一）
        let code = tool_call
            .parameters
            .get("code")
            .or_else(|| tool_call.parameters.get("stdin"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("firecracker_executor requires 'code' or 'stdin'"))?
            .to_string();

        let session_id = tool_call
            .parameters
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                session_context.and_then(|ctx| {
                    ctx.fields.get("session_id").and_then(|v| {
                        if let Some(prost_types::value::Kind::StringValue(s)) = &v.kind {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                })
            });

        info!(
            "Firecracker execute: session_id={:?}, context_keys={:?}",
            session_id,
            session_context.map(|c| c.fields.keys().collect::<Vec<_>>())
        );

        let timeout_seconds = tool_call
            .parameters
            .get("timeout_seconds")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        let workspace_path = if let Some(sid) = &session_id {
            let wm = WorkspaceManager::from_env();
            wm.get_workspace(sid)
                .ok()
                .map(|p| p.to_string_lossy().to_string())
        } else {
            None
        };

        let req = FirecrackerExecuteRequest {
            code,
            session_id,
            timeout_seconds,
            workspace_path,
            stdin: tool_call
                .parameters
                .get("stdin")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        match client.execute(req).await {
            Ok(resp) => {
                if resp.success {
                    Ok(ToolResult {
                        tool: tool_call.tool_name.clone(),
                        success: true,
                        output: serde_json::Value::String(resp.stdout),
                        error: None,
                    })
                } else {
                    let err = resp
                        .error
                        .or_else(|| {
                            if resp.stderr.is_empty() {
                                None
                            } else {
                                Some(resp.stderr)
                            }
                        })
                        .unwrap_or_else(|| "Firecracker execution failed".to_string());
                    Ok(ToolResult {
                        tool: tool_call.tool_name.clone(),
                        success: false,
                        output: serde_json::Value::Null,
                        error: Some(err),
                    })
                }
            }
            Err(e) if Self::is_transient_error(&e) && !self.disable_wasi_fallback => {
                warn!("Firecracker transient error, falling back to WASI: {}", e);
                self.execute_wasi_fallback(tool_call, session_context).await
            }
            Err(e) if Self::is_transient_error(&e) => {
                // WASI fallback disabled (EKS mode) - fail fast
                // WASI 回退已禁用（EKS 模式）——直接失败
                error!("Firecracker unavailable (WASI fallback disabled): {}", e);
                Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some(format!("Python executor unavailable: {}", e)),
                })
            }
            Err(e) => Ok(ToolResult {
                tool: tool_call.tool_name.clone(),
                success: false,
                output: serde_json::Value::Null,
                error: Some(format!("Firecracker executor error: {}", e)),
            }),
        }
    }

    /// Check if an error is transient and should trigger WASI fallback.
    /// 判断错误是否为瞬时错误、应触发 WASI 回退。
    /// Returns true for connection refused, timeout, and 503 Service Unavailable.
    /// 连接被拒绝、timeout 以及 503 Service Unavailable 时返回 true。
    fn is_transient_error(e: &anyhow::Error) -> bool {
        let err_str = e.to_string().to_lowercase();

        // Connection refused
        // 连接被拒绝
        if err_str.contains("connection refused") {
            return true;
        }

        // Timeout errors
        // timeout 错误
        if err_str.contains("timeout") || err_str.contains("timed out") {
            return true;
        }

        // 503 Service Unavailable
        // 503 服务不可用
        if err_str.contains("503") || err_str.contains("service unavailable") {
            return true;
        }

        // DNS resolution failure
        // DNS 解析失败
        if err_str.contains("dns") || err_str.contains("name resolution") {
            return true;
        }

        false
    }

    /// Execute code using WASI sandbox as fallback when Firecracker is unavailable.
    /// Firecracker 不可用时，用 WASI sandbox 作为回退执行代码。
    /// This provides a degraded but functional execution path.
    /// 提供一条降级但仍可用的执行路径。
    #[cfg(feature = "wasi")]
    async fn execute_wasi_fallback(
        &self,
        tool_call: &ToolCall,
        _session_context: Option<&prost_types::Struct>,
    ) -> Result<ToolResult> {
        // Get the Python code from parameters
        // 从参数中获取 Python 代码
        let code = match tool_call.parameters.get("code").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => {
                return Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some("WASI fallback requires 'code' parameter".to_string()),
                });
            }
        };

        // Check if WASI sandbox is available
        // 检查 WASI sandbox 是否可用
        let wasi = match &self.wasi {
            Some(w) => w,
            None => {
                warn!("WASI sandbox not configured for fallback execution");
                return Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some(
                        "Neither Firecracker nor WASI sandbox available for code execution"
                            .to_string(),
                    ),
                });
            }
        };

        // Get Python WASM interpreter path from environment
        // 从环境变量获取 Python WASM 解释器路径
        let wasm_path = std::env::var("PYTHON_WASI_WASM_PATH")
            .unwrap_or_else(|_| "/opt/wasm-interpreters/python-3.11.4.wasm".to_string());

        // Read the Python WASM interpreter
        // 读取 Python WASM 解释器
        let wasm_bytes = match fs::read(&wasm_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(
                    "Failed to read Python WASM interpreter at {}: {}",
                    wasm_path, e
                );
                return Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some(format!(
                        "python_executor unavailable: WASM interpreter not installed at {}. Use file_write to save code to a file instead.",
                        wasm_path
                    )),
                });
            }
        };

        // Execute Python code via WASI with the code as stdin
        // 通过 WASI 执行 Python 代码，代码经 stdin 传入
        // The Python interpreter reads from stdin when given -c flag
        // 带 -c 参数时 Python 解释器从 stdin 读取代码
        let argv = Some(vec![
            "python".to_string(),
            "-c".to_string(),
            code.to_string(),
        ]);

        match wasi.execute_wasm_with_args(&wasm_bytes, "", argv).await {
            Ok(output) => {
                info!("WASI fallback execution successful");
                Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: true,
                    output: serde_json::Value::String(output),
                    error: None,
                })
            }
            Err(e) => {
                let msg = format!("WASI fallback execution error: {}", e);
                warn!("{}", msg);
                Ok(ToolResult {
                    tool: tool_call.tool_name.clone(),
                    success: false,
                    output: serde_json::Value::Null,
                    error: Some(msg),
                })
            }
        }
    }

    /// Fallback stub when WASI feature is disabled.
    /// 未启用 WASI feature 时的回退桩实现。
    #[cfg(not(feature = "wasi"))]
    async fn execute_wasi_fallback(
        &self,
        tool_call: &ToolCall,
        _session_context: Option<&prost_types::Struct>,
    ) -> Result<ToolResult> {
        warn!("WASI feature disabled, cannot provide fallback for Firecracker");
        Ok(ToolResult {
            tool: tool_call.tool_name.clone(),
            success: false,
            output: serde_json::Value::Null,
            error: Some("Firecracker unavailable and WASI fallback not compiled".to_string()),
        })
    }
}
