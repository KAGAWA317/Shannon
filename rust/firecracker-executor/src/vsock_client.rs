use crate::models::{GuestRequest, GuestResponse};
use anyhow::Result;
use std::path::PathBuf;
use tracing::{debug, warn};

/// Maximum number of connection retry attempts
/// 连接 retry 的最大次数
const MAX_CONNECT_RETRIES: u32 = 10;
/// Initial backoff delay in milliseconds
/// 初始退避延迟，单位毫秒
const INITIAL_BACKOFF_MS: u64 = 200;
/// Maximum backoff delay in milliseconds
/// 最大退避延迟，单位毫秒
const MAX_BACKOFF_MS: u64 = 2000;

/// Execute code in a Firecracker guest via vsock UDS proxy.
/// 通过 vsock UDS 代理在 Firecracker guest 中执行代码。
///
/// Firecracker's vsock works via a UDS socket on the host. To connect:
/// Firecracker 的 vsock 在宿主侧通过 UDS socket 工作，连接步骤：
/// 1. Connect to the UDS socket
/// 1. 连接 UDS socket
/// 2. Send: "CONNECT <port>\n"
/// 2. 发送："CONNECT <port>\n"
/// 3. Receive: "OK <local_port>\n"
/// 3. 接收："OK <local_port>\n"
/// 4. Then communicate with the guest
/// 4. 随后与 guest 通信
///
/// Note: Per Firecracker issue #1253, the host-side UDS connect succeeds immediately
/// 注意：根据 Firecracker issue #1253，宿主侧 UDS 连接会立即成功，
/// but Firecracker may terminate the connection if the guest isn't ready yet.
/// 但若 guest 尚未就绪，Firecracker 可能随后终止连接。
/// We implement retry logic with exponential backoff to handle this.
/// 因此实现带指数退避的 retry 逻辑来处理该情况。
#[cfg(target_os = "linux")]
pub async fn execute_guest_via_uds(
    vsock_uds: &PathBuf,
    port: u32,
    req: GuestRequest,
    timeout_seconds: u32,
) -> Result<GuestResponse> {
    use anyhow::anyhow;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;
    use tokio::time::timeout;

    let uds_path = vsock_uds.clone();
    let deadline = Duration::from_secs(timeout_seconds as u64);

    let resp = timeout(deadline, tokio::task::spawn_blocking(move || -> Result<GuestResponse> {
        let per_attempt_timeout = Duration::from_secs(5);
        let mut last_error: Option<anyhow::Error> = None;
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        // Retry loop with exponential backoff
        // 带指数退避的 retry 循环
        // Per Firecracker issue #1253, the host UDS connect succeeds immediately but
        // 根据 Firecracker issue #1253，宿主侧 UDS 连接会立即成功，
        // Firecracker terminates the connection if guest init hasn't bound the vsock port yet
        // 但若 guest init 尚未绑定 vsock 端口，Firecracker 会终止连接
        for attempt in 1..=MAX_CONNECT_RETRIES {
            match try_connect_and_execute(&uds_path, port, &req, per_attempt_timeout) {
                Ok(resp) => {
                    if attempt > 1 {
                        debug!(attempt = attempt, "vsock connection succeeded after retries");
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Check if this is a transient connection error (guest not ready)
                    // 判断是否为瞬时连接错误（guest 未就绪）
                    let is_transient = err_str.contains("connection refused")
                        || err_str.contains("EOF")
                        || err_str.contains("empty response")
                        || err_str.contains("UnexpectedEof")
                        || err_str.contains("vsock CONNECT failed")
                        || err_str.contains("broken pipe")
                        || err_str.contains("Connection reset");

                    if is_transient && attempt < MAX_CONNECT_RETRIES {
                        warn!(
                            attempt = attempt,
                            max = MAX_CONNECT_RETRIES,
                            backoff_ms = backoff_ms,
                            error = %err_str,
                            "vsock connection failed, retrying (guest may not be ready)"
                        );
                        std::thread::sleep(Duration::from_millis(backoff_ms));
                        // Exponential backoff with cap
                        // 带上限的指数退避
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                        last_error = Some(e);
                    } else {
                        // Non-transient error or max retries reached
                        // 非瞬时错误或已达最大 retry 次数
                        return Err(e);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("vsock connection failed after {} retries", MAX_CONNECT_RETRIES)))
    }))
    .await??;

    resp
}

/// Single connection attempt to guest via vsock
/// 通过 vsock 向 guest 发起单次连接尝试
#[cfg(target_os = "linux")]
fn try_connect_and_execute(
    uds_path: &PathBuf,
    port: u32,
    req: &GuestRequest,
    timeout: std::time::Duration,
) -> Result<GuestResponse> {
    use anyhow::anyhow;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;

    // Connect to Firecracker's vsock UDS
    // 连接 Firecracker 的 vsock UDS
    let mut stream = UnixStream::connect(uds_path)
        .map_err(|e| anyhow!("vsock UDS connect failed: {}", e))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();

    // Send CONNECT command to establish connection to guest port
    // 发送 CONNECT 命令，建立到 guest 端口的连接
    let connect_cmd = format!("CONNECT {}\n", port);
    stream.write_all(connect_cmd.as_bytes())?;
    stream.flush()?;

    // Read OK response - this is where connection fails if guest isn't ready
    // 读取 OK response——guest 未就绪时连接会在这里失败
    // Firecracker will terminate the socket causing EOF or empty read
    // Firecracker 会终止 socket，导致 EOF 或空读取
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut response_line = String::new();
    let bytes_read = reader.read_line(&mut response_line)?;

    if bytes_read == 0 {
        return Err(anyhow!("vsock CONNECT failed: EOF (guest not ready)"));
    }
    if !response_line.starts_with("OK") {
        return Err(anyhow!("vsock CONNECT failed: {}", response_line.trim()));
    }

    // Get the underlying stream back for writing
    // 取回底层 stream 用于写入
    let mut stream = reader.into_inner();

    // Send the request as JSON
    // 以 JSON 发送 request
    serde_json::to_writer(&mut stream, req)?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    // Read response
    // 读取 response
    let mut buf = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut buf)?;

    if buf.trim().is_empty() {
        return Err(anyhow!("empty response from guest"));
    }
    let parsed: GuestResponse = serde_json::from_str(buf.trim())?;
    Ok(parsed)
}

/// Legacy function signature for compatibility - now requires UDS path
/// 兼容用的旧函数签名——现在需要 UDS 路径
#[cfg(target_os = "linux")]
pub async fn execute_guest(
    _cid: u32,
    _port: u32,
    _req: GuestRequest,
    _timeout_seconds: u32,
) -> Result<GuestResponse> {
    Err(anyhow::anyhow!(
        "execute_guest requires UDS path - use execute_guest_via_uds instead"
    ))
}

#[cfg(not(target_os = "linux"))]
pub async fn execute_guest(
    _cid: u32,
    _port: u32,
    _req: GuestRequest,
    _timeout_seconds: u32,
) -> Result<GuestResponse> {
    Err(anyhow::anyhow!(
        "vsock is only supported on Linux hosts"
    ))
}

#[cfg(not(target_os = "linux"))]
pub async fn execute_guest_via_uds(
    _vsock_uds: &PathBuf,
    _port: u32,
    _req: GuestRequest,
    _timeout_seconds: u32,
) -> Result<GuestResponse> {
    Err(anyhow::anyhow!(
        "Firecracker executor is only supported on Linux hosts"
    ))
}
