use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    // Ensure a usable `protoc` is available (vendored fallback)
    // 确保有可用的 `protoc`（内置 vendored 版本作为兜底）
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(pb) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", pb);
        }
    }
    // Determine proto path - check if we're in Docker or local
    // 确定 proto 路径——判断当前在 Docker 还是本地环境
    let proto_path = if Path::new("/protos").exists() {
        // Docker environment
        // Docker 环境
        "/protos"
    } else {
        // Local development
        // 本地开发环境
        "../../protos"
    };

    let common_proto = format!("{}/common/common.proto", proto_path);
    let agent_proto = format!("{}/agent/agent.proto", proto_path);
    let sandbox_proto = format!("{}/sandbox/sandbox.proto", proto_path);

    // Compile protobuf files with reflection support
    // 编译 protobuf 文件并支持反射
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(std::env::var("OUT_DIR").unwrap() + "/shannon_descriptor.bin")
        .compile_protos(
            &[&common_proto, &agent_proto, &sandbox_proto],
            &[proto_path],
        )?;
    Ok(())
}
