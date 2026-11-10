//! Safe command implementations for WASI sandbox.
//! WASI sandbox 的安全命令实现。
//!
//! These commands are implemented natively in Rust instead of spawning
//! 这些命令用 Rust 原生实现，而非启动
//! shell processes, eliminating shell injection risks entirely.
//! shell 进程，从而彻底消除 shell 注入风险。

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use tracing::debug;

/// Safe commands that can be executed in the sandbox.
/// 可在 sandbox 中执行的安全命令。
#[derive(Debug, Clone)]
pub enum SafeCommand {
    /// List directory contents
    /// 列出目录内容
    Ls {
        path: String,
        all: bool,  // -a: show hidden
        long: bool, // -l: long format
    },
    /// Print file contents
    /// 输出文件内容
    Cat { path: String },
    /// Print first N lines
    /// 输出前 N 行
    Head { path: String, lines: usize },
    /// Print last N lines
    /// 输出后 N 行
    Tail { path: String, lines: usize },
    /// Count lines/words/bytes
    /// 统计行数/词数/字节数
    Wc { path: String },
    /// Create directory
    /// 创建目录
    Mkdir { path: String, parents: bool },
    /// Remove file or directory
    /// 删除文件或目录
    Rm { path: String, recursive: bool },
    /// Copy file
    /// 复制文件
    Cp { src: String, dst: String },
    /// Move/rename file
    /// 移动/重命名文件
    Mv { src: String, dst: String },
    /// Create empty file
    /// 创建空文件
    Touch { path: String },
    /// Print working directory
    /// 输出当前工作目录
    Pwd,
    /// Print text
    /// 输出文本
    Echo { text: String },
    /// Search for pattern in files
    /// 在文件中搜索模式
    Grep {
        pattern: String,
        path: String,
        ignore_case: bool,
    },
    /// Find files by name
    /// 按名称查找文件
    Find { path: String, name: String },
}

impl SafeCommand {
    /// Shell metacharacters that indicate command injection attempts
    /// 可能表示命令注入尝试的 shell 元字符
    const DANGEROUS_PATTERNS: &'static [&'static str] =
        &["|", ";", "&&", "||", ">", "<", ">>", "$(", "`", "\n", "\r"];

    /// Parse a command string into a SafeCommand.
    /// 将命令字符串解析为 SafeCommand。
    pub fn parse(input: &str) -> Result<SafeCommand> {
        // First, reject any dangerous shell metacharacters
        // 先拒绝任何危险的 shell 元字符
        for pattern in Self::DANGEROUS_PATTERNS {
            if input.contains(pattern) {
                return Err(anyhow!("Shell metacharacter not allowed: {}", pattern));
            }
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return Err(anyhow!("Empty command"));
        }

        let cmd = parts[0];
        let args = &parts[1..];

        match cmd {
            "ls" => Self::parse_ls(args),
            "cat" => Self::parse_cat(args),
            "head" => Self::parse_head(args),
            "tail" => Self::parse_tail(args),
            "wc" => Self::parse_wc(args),
            "mkdir" => Self::parse_mkdir(args),
            "rm" => Self::parse_rm(args),
            "cp" => Self::parse_cp(args),
            "mv" => Self::parse_mv(args),
            "touch" => Self::parse_touch(args),
            "pwd" => Ok(SafeCommand::Pwd),
            "echo" => Ok(SafeCommand::Echo {
                text: args.join(" "),
            }),
            "grep" => Self::parse_grep(args),
            "find" => Self::parse_find(args),
            _ => Err(anyhow!("Command not allowed: {}", cmd)),
        }
    }

    fn parse_ls(args: &[&str]) -> Result<SafeCommand> {
        let mut path = ".".to_string();
        let mut all = false;
        let mut long = false;

        for arg in args {
            if arg.starts_with('-') {
                if arg.contains('a') {
                    all = true;
                }
                if arg.contains('l') {
                    long = true;
                }
            } else {
                path = arg.to_string();
            }
        }

        Ok(SafeCommand::Ls { path, all, long })
    }

    fn parse_cat(args: &[&str]) -> Result<SafeCommand> {
        if args.is_empty() {
            return Err(anyhow!("cat requires a file path"));
        }
        Ok(SafeCommand::Cat {
            path: args[0].to_string(),
        })
    }

    fn parse_head(args: &[&str]) -> Result<SafeCommand> {
        let mut path = String::new();
        let mut lines = 10;

        let mut i = 0;
        while i < args.len() {
            if args[i] == "-n" && i + 1 < args.len() {
                lines = args[i + 1].parse().unwrap_or(10);
                i += 2;
            } else if !args[i].starts_with('-') {
                path = args[i].to_string();
                i += 1;
            } else {
                i += 1;
            }
        }

        if path.is_empty() {
            return Err(anyhow!("head requires a file path"));
        }
        Ok(SafeCommand::Head { path, lines })
    }

    fn parse_tail(args: &[&str]) -> Result<SafeCommand> {
        let mut path = String::new();
        let mut lines = 10;

        let mut i = 0;
        while i < args.len() {
            if args[i] == "-n" && i + 1 < args.len() {
                lines = args[i + 1].parse().unwrap_or(10);
                i += 2;
            } else if !args[i].starts_with('-') {
                path = args[i].to_string();
                i += 1;
            } else {
                i += 1;
            }
        }

        if path.is_empty() {
            return Err(anyhow!("tail requires a file path"));
        }
        Ok(SafeCommand::Tail { path, lines })
    }

    fn parse_wc(args: &[&str]) -> Result<SafeCommand> {
        let path = args.iter().find(|a| !a.starts_with('-'));
        match path {
            Some(p) => Ok(SafeCommand::Wc {
                path: p.to_string(),
            }),
            None => Err(anyhow!("wc requires a file path")),
        }
    }

    fn parse_mkdir(args: &[&str]) -> Result<SafeCommand> {
        let mut path = String::new();
        let mut parents = false;

        for arg in args {
            if *arg == "-p" {
                parents = true;
            } else if !arg.starts_with('-') {
                path = arg.to_string();
            }
        }

        if path.is_empty() {
            return Err(anyhow!("mkdir requires a path"));
        }
        Ok(SafeCommand::Mkdir { path, parents })
    }

    fn parse_rm(args: &[&str]) -> Result<SafeCommand> {
        let mut path = String::new();
        let mut recursive = false;

        for arg in args {
            if arg.contains('r') && arg.starts_with('-') {
                recursive = true;
            } else if !arg.starts_with('-') {
                path = arg.to_string();
            }
        }

        if path.is_empty() {
            return Err(anyhow!("rm requires a path"));
        }
        Ok(SafeCommand::Rm { path, recursive })
    }

    fn parse_cp(args: &[&str]) -> Result<SafeCommand> {
        let non_flag: Vec<&str> = args
            .iter()
            .filter(|a| !a.starts_with('-'))
            .copied()
            .collect();
        if non_flag.len() < 2 {
            return Err(anyhow!("cp requires source and destination"));
        }
        Ok(SafeCommand::Cp {
            src: non_flag[0].to_string(),
            dst: non_flag[1].to_string(),
        })
    }

    fn parse_mv(args: &[&str]) -> Result<SafeCommand> {
        let non_flag: Vec<&str> = args
            .iter()
            .filter(|a| !a.starts_with('-'))
            .copied()
            .collect();
        if non_flag.len() < 2 {
            return Err(anyhow!("mv requires source and destination"));
        }
        Ok(SafeCommand::Mv {
            src: non_flag[0].to_string(),
            dst: non_flag[1].to_string(),
        })
    }

    fn parse_touch(args: &[&str]) -> Result<SafeCommand> {
        let path = args.iter().find(|a| !a.starts_with('-'));
        match path {
            Some(p) => Ok(SafeCommand::Touch {
                path: p.to_string(),
            }),
            None => Err(anyhow!("touch requires a path")),
        }
    }

    fn parse_grep(args: &[&str]) -> Result<SafeCommand> {
        let mut pattern = String::new();
        let mut path = String::new();
        let mut ignore_case = false;

        let mut i = 0;
        while i < args.len() {
            if args[i] == "-i" {
                ignore_case = true;
                i += 1;
            } else if !args[i].starts_with('-') {
                if pattern.is_empty() {
                    pattern = args[i].to_string();
                } else {
                    path = args[i].to_string();
                }
                i += 1;
            } else {
                i += 1;
            }
        }

        if pattern.is_empty() || path.is_empty() {
            return Err(anyhow!("grep requires pattern and file path"));
        }
        Ok(SafeCommand::Grep {
            pattern,
            path,
            ignore_case,
        })
    }

    fn parse_find(args: &[&str]) -> Result<SafeCommand> {
        let mut path = ".".to_string();
        let mut name = String::new();

        let mut i = 0;
        while i < args.len() {
            if args[i] == "-name" && i + 1 < args.len() {
                name = args[i + 1].to_string();
                i += 2;
            } else if !args[i].starts_with('-') {
                path = args[i].to_string();
                i += 1;
            } else {
                i += 1;
            }
        }

        Ok(SafeCommand::Find { path, name })
    }

    /// Execute the command within a workspace directory.
    /// 在工作区目录内执行命令。
    pub fn execute(&self, workspace: &Path) -> Result<CommandOutput> {
        self.execute_with_memory(workspace, None)
    }

    /// Execute the command with optional memory workspace access for /memory paths.
    /// 执行命令，可选提供记忆工作区以访问 /memory 路径。
    pub fn execute_with_memory(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
    ) -> Result<CommandOutput> {
        debug!("Executing {:?} in {:?}", self, workspace);

        match self {
            SafeCommand::Ls { path, all, long } => {
                self.exec_ls(workspace, memory_workspace, path, *all, *long)
            }
            SafeCommand::Cat { path } => self.exec_cat(workspace, memory_workspace, path),
            SafeCommand::Head { path, lines } => {
                self.exec_head(workspace, memory_workspace, path, *lines)
            }
            SafeCommand::Tail { path, lines } => {
                self.exec_tail(workspace, memory_workspace, path, *lines)
            }
            SafeCommand::Wc { path } => self.exec_wc(workspace, memory_workspace, path),
            SafeCommand::Mkdir { path, parents } => {
                self.exec_mkdir(workspace, memory_workspace, path, *parents)
            }
            SafeCommand::Rm { path, recursive } => {
                self.exec_rm(workspace, memory_workspace, path, *recursive)
            }
            SafeCommand::Cp { src, dst } => self.exec_cp(workspace, memory_workspace, src, dst),
            SafeCommand::Mv { src, dst } => self.exec_mv(workspace, memory_workspace, src, dst),
            SafeCommand::Touch { path } => self.exec_touch(workspace, memory_workspace, path),
            SafeCommand::Pwd => Ok(CommandOutput::success(
                workspace.to_string_lossy().to_string(),
            )),
            SafeCommand::Echo { text } => Ok(CommandOutput::success(text.clone())),
            SafeCommand::Grep {
                pattern,
                path,
                ignore_case,
            } => self.exec_grep(workspace, memory_workspace, pattern, path, *ignore_case),
            SafeCommand::Find { path, name } => {
                self.exec_find(workspace, memory_workspace, path, name)
            }
        }
    }

    /// Check if the command touches the /memory mount.
    /// 判断命令是否涉及 /memory 挂载点。
    pub fn uses_memory(&self) -> bool {
        match self {
            Self::Ls { path, .. }
            | Self::Cat { path }
            | Self::Head { path, .. }
            | Self::Tail { path, .. }
            | Self::Wc { path }
            | Self::Mkdir { path, .. }
            | Self::Rm { path, .. }
            | Self::Touch { path }
            | Self::Grep { path, .. } => Self::is_memory_path(path),
            Self::Cp { src, dst } | Self::Mv { src, dst } => {
                Self::is_memory_path(src) || Self::is_memory_path(dst)
            }
            Self::Find { path, .. } => Self::is_memory_path(path),
            Self::Pwd | Self::Echo { .. } => false,
        }
    }

    fn is_memory_path(path: &str) -> bool {
        path == "/memory" || path.starts_with("/memory/")
    }

    fn resolve_path(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        relative: &str,
    ) -> Result<PathBuf> {
        let canonical_workspace = workspace
            .canonicalize()
            .unwrap_or_else(|_| workspace.to_path_buf());

        // Support paths that explicitly target the Firecracker mount.
        // 支持显式指向 Firecracker 挂载点的路径。
        let normalized = relative
            .strip_prefix("/workspace/")
            .or_else(|| relative.strip_prefix("/workspace"))
            .unwrap_or(relative);

        // Support memory mount when explicitly requested.
        // 显式 request 时支持记忆挂载点。
        if Self::is_memory_path(normalized) {
            if memory_workspace.is_none() {
                return Err(anyhow!("Cannot resolve /memory path without user_id"));
            }

            let memory_workspace = memory_workspace.expect("Checked above");
            let canonical_memory = memory_workspace
                .canonicalize()
                .unwrap_or_else(|_| memory_workspace.to_path_buf());
            let memory_subpath = normalized
                .strip_prefix("/memory/")
                .or_else(|| normalized.strip_prefix("/memory"))
                .unwrap_or("");

            if memory_subpath.is_empty() {
                return Ok(canonical_memory);
            }

            if memory_subpath.contains("..") {
                return Err(anyhow!("Path traversal not allowed in /memory"));
            }

            let target = canonical_memory.join(memory_subpath);
            if target.exists() {
                let canonical = target.canonicalize()?;
                if !canonical.starts_with(&canonical_memory) {
                    return Err(anyhow!("Path escapes memory directory"));
                }
                return Ok(canonical);
            }

            if let Some(parent) = target.parent() {
                if parent.exists() {
                    let canonical_parent = parent.canonicalize()?;
                    if !canonical_parent.starts_with(&canonical_memory) {
                        return Err(anyhow!("Path escapes memory directory"));
                    }
                }
            }

            return Ok(target);
        }

        // Canonicalize workspace first to handle symlinks (e.g., /var -> /private/var on macOS)
        // 先规范化工作区以处理符号链接（例如 macOS 上 /var -> /private/var）
        let target = if normalized.is_empty() {
            canonical_workspace.clone()
        } else {
            // Security: Reject absolute paths in the workspace namespace after normalization.
            // 安全：归一化后拒绝工作区命名空间中的绝对路径。
            let req_path = Path::new(normalized);
            if req_path.is_absolute() {
                return Err(anyhow!("Absolute paths are not allowed"));
            }

            canonical_workspace.join(normalized)
        };

        // For existing paths, canonicalize
        // 对已存在的路径执行规范化
        if target.exists() {
            let canonical = target.canonicalize()?;
            if !canonical.starts_with(&canonical_workspace) {
                return Err(anyhow!("Path escapes workspace"));
            }
            return Ok(canonical);
        }

        // For non-existing paths, validate parent
        // 对不存在的路径，校验其父目录
        if let Some(parent) = target.parent() {
            if parent.exists() {
                let canonical_parent = parent.canonicalize()?;
                if !canonical_parent.starts_with(&canonical_workspace) {
                    return Err(anyhow!("Path escapes workspace"));
                }
            }
        }

        Ok(target)
    }

    fn exec_ls(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        all: bool,
        long: bool,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;

        if !target.is_dir() {
            return Err(anyhow!("Not a directory: {}", path));
        }

        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&target)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden files unless -a
            // 除非指定 -a，否则跳过隐藏文件
            if !all && name.starts_with('.') {
                continue;
            }

            if long {
                let meta = entry.metadata()?;
                let size = meta.len();
                let file_type = if meta.is_dir() { "d" } else { "-" };
                entries.push(format!("{} {:>10} {}", file_type, size, name));
            } else {
                entries.push(name);
            }
        }

        entries.sort();
        Ok(CommandOutput::success(entries.join("\n")))
    }

    fn exec_cat(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let content = std::fs::read_to_string(&target)?;
        Ok(CommandOutput::success(content))
    }

    fn exec_head(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        lines: usize,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let content = std::fs::read_to_string(&target)?;
        let output: String = content.lines().take(lines).collect::<Vec<_>>().join("\n");
        Ok(CommandOutput::success(output))
    }

    fn exec_tail(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        lines: usize,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let content = std::fs::read_to_string(&target)?;
        let all_lines: Vec<&str> = content.lines().collect();
        let start = all_lines.len().saturating_sub(lines);
        let output = all_lines[start..].join("\n");
        Ok(CommandOutput::success(output))
    }

    fn exec_wc(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let content = std::fs::read_to_string(&target)?;
        let lines = content.lines().count();
        let words = content.split_whitespace().count();
        let bytes = content.len();
        Ok(CommandOutput::success(format!(
            "{:>8} {:>8} {:>8} {}",
            lines, words, bytes, path
        )))
    }

    fn exec_mkdir(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        parents: bool,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;

        if parents {
            std::fs::create_dir_all(&target)?;
        } else {
            std::fs::create_dir(&target)?;
        }
        Ok(CommandOutput::success(String::new()))
    }

    fn exec_rm(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        recursive: bool,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;

        if target.is_dir() {
            if recursive {
                std::fs::remove_dir_all(&target)?;
            } else {
                std::fs::remove_dir(&target)?;
            }
        } else {
            std::fs::remove_file(&target)?;
        }
        Ok(CommandOutput::success(String::new()))
    }

    fn exec_cp(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        src: &str,
        dst: &str,
    ) -> Result<CommandOutput> {
        let src_path = self.resolve_path(workspace, memory_workspace, src)?;
        let dst_path = self.resolve_path(workspace, memory_workspace, dst)?;
        std::fs::copy(&src_path, &dst_path)?;
        Ok(CommandOutput::success(String::new()))
    }

    fn exec_mv(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        src: &str,
        dst: &str,
    ) -> Result<CommandOutput> {
        let src_path = self.resolve_path(workspace, memory_workspace, src)?;
        let dst_path = self.resolve_path(workspace, memory_workspace, dst)?;
        std::fs::rename(&src_path, &dst_path)?;
        Ok(CommandOutput::success(String::new()))
    }

    fn exec_touch(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        if !target.exists() {
            std::fs::File::create(&target)?;
        }
        Ok(CommandOutput::success(String::new()))
    }

    fn exec_grep(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        pattern: &str,
        path: &str,
        ignore_case: bool,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let content = std::fs::read_to_string(&target)?;

        let matches: Vec<&str> = content
            .lines()
            .filter(|line| {
                if ignore_case {
                    line.to_lowercase().contains(&pattern.to_lowercase())
                } else {
                    line.contains(pattern)
                }
            })
            .collect();

        Ok(CommandOutput::success(matches.join("\n")))
    }

    fn exec_find(
        &self,
        workspace: &Path,
        memory_workspace: Option<&Path>,
        path: &str,
        name: &str,
    ) -> Result<CommandOutput> {
        let target = self.resolve_path(workspace, memory_workspace, path)?;
        let mut results = Vec::new();

        fn walk(dir: &Path, name: &str, workspace: &Path, results: &mut Vec<String>) -> Result<()> {
            if dir.is_dir() {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let entry_path = entry.path();
                    let entry_name = entry.file_name().to_string_lossy().to_string();

                    if name.is_empty() || entry_name.contains(name) || glob_match(name, &entry_name)
                    {
                        let relative = entry_path.strip_prefix(workspace).unwrap_or(&entry_path);
                        results.push(relative.to_string_lossy().to_string());
                    }

                    if entry_path.is_dir() {
                        walk(&entry_path, name, workspace, results)?;
                    }
                }
            }
            Ok(())
        }

        walk(&target, name, workspace, &mut results)?;
        results.sort();
        Ok(CommandOutput::success(results.join("\n")))
    }
}

/// Simple glob matching for find command (no regex to avoid DoS).
/// find 命令的简单 glob 匹配（不用正则，避免 DoS）。
fn glob_match(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    glob_match_recursive(pattern.as_bytes(), name.as_bytes())
}

/// Recursive glob matcher without regex (prevents ReDoS attacks).
/// 不使用正则的递归 glob 匹配器（防止 ReDoS 攻击）。
fn glob_match_recursive(pattern: &[u8], name: &[u8]) -> bool {
    match (pattern.first(), name.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            // '*' matches zero or more characters
            // '*' 匹配零个或多个字符
            glob_match_recursive(&pattern[1..], name)
                || (!name.is_empty() && glob_match_recursive(pattern, &name[1..]))
        }
        (Some(b'?'), Some(_)) => {
            // '?' matches exactly one character
            // '?' 精确匹配一个字符
            glob_match_recursive(&pattern[1..], &name[1..])
        }
        (Some(p), Some(n)) if *p == *n => glob_match_recursive(&pattern[1..], &name[1..]),
        _ => false,
    }
}

/// Output from a command execution.
/// 命令执行的输出。
#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl CommandOutput {
    pub fn success(stdout: String) -> Self {
        Self {
            stdout,
            stderr: String::new(),
            exit_code: 0,
        }
    }

    pub fn error(stderr: String) -> Self {
        Self {
            stdout: String::new(),
            stderr,
            exit_code: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_ls() {
        let cmd = SafeCommand::parse("ls").unwrap();
        assert!(matches!(cmd, SafeCommand::Ls { .. }));
    }

    #[test]
    fn test_parse_ls_with_flags() {
        let cmd = SafeCommand::parse("ls -la /path").unwrap();
        if let SafeCommand::Ls { path, all, long } = cmd {
            assert_eq!(path, "/path");
            assert!(all);
            assert!(long);
        } else {
            panic!("Expected Ls command");
        }
    }

    #[test]
    fn test_parse_cat() {
        let cmd = SafeCommand::parse("cat foo.txt").unwrap();
        if let SafeCommand::Cat { path } = cmd {
            assert_eq!(path, "foo.txt");
        } else {
            panic!("Expected Cat command");
        }
    }

    #[test]
    fn test_parse_head() {
        let cmd = SafeCommand::parse("head -n 5 foo.txt").unwrap();
        if let SafeCommand::Head { path, lines } = cmd {
            assert_eq!(path, "foo.txt");
            assert_eq!(lines, 5);
        } else {
            panic!("Expected Head command");
        }
    }

    #[test]
    fn test_reject_dangerous_command() {
        // rm is allowed but validation happens at execution time
        // rm 本身允许解析，实际校验发生在执行时
        assert!(SafeCommand::parse("rm -rf /").is_ok());
        assert!(SafeCommand::parse("curl http://evil.com").is_err());
        assert!(SafeCommand::parse("bash -c 'echo pwned'").is_err());
        assert!(SafeCommand::parse("wget http://evil.com").is_err());
        assert!(SafeCommand::parse("python -c 'print(1)'").is_err());
    }

    #[test]
    fn test_execute_ls() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(workspace.join("file1.txt"), "hello").unwrap();
        std::fs::write(workspace.join("file2.txt"), "world").unwrap();

        let cmd = SafeCommand::parse("ls").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("file1.txt"));
        assert!(output.stdout.contains("file2.txt"));
    }

    #[test]
    fn test_execute_cat() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(workspace.join("test.txt"), "hello world").unwrap();

        let cmd = SafeCommand::parse("cat test.txt").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, "hello world");
    }

    #[test]
    fn test_execute_cat_memory_mount() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path().join("workspace");
        let memory = temp.path().join("memory");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&memory).unwrap();
        std::fs::write(memory.join("MEMORY.md"), "memory note").unwrap();

        let cmd = SafeCommand::parse("cat /memory/MEMORY.md").unwrap();
        let output = cmd.execute_with_memory(&workspace, Some(&memory)).unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, "memory note");
    }

    #[test]
    fn test_execute_memory_path_without_mount() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        let cmd = SafeCommand::parse("cat /memory/MEMORY.md").unwrap();
        let result = cmd.execute_with_memory(workspace, None);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot resolve /memory path without user_id"
        );
    }

    #[test]
    fn test_execute_head() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(
            workspace.join("lines.txt"),
            "line1\nline2\nline3\nline4\nline5",
        )
        .unwrap();

        let cmd = SafeCommand::parse("head -n 3 lines.txt").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, "line1\nline2\nline3");
    }

    #[test]
    fn test_execute_tail() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(
            workspace.join("lines.txt"),
            "line1\nline2\nline3\nline4\nline5",
        )
        .unwrap();

        let cmd = SafeCommand::parse("tail -n 2 lines.txt").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, "line4\nline5");
    }

    #[test]
    fn test_execute_mkdir_and_touch() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        let cmd = SafeCommand::parse("mkdir -p subdir/nested").unwrap();
        cmd.execute(workspace).unwrap();
        assert!(workspace.join("subdir/nested").is_dir());

        let cmd = SafeCommand::parse("touch subdir/nested/file.txt").unwrap();
        cmd.execute(workspace).unwrap();
        assert!(workspace.join("subdir/nested/file.txt").exists());
    }

    #[test]
    fn test_execute_cp_and_mv() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(workspace.join("original.txt"), "content").unwrap();

        let cmd = SafeCommand::parse("cp original.txt copy.txt").unwrap();
        cmd.execute(workspace).unwrap();
        assert!(workspace.join("copy.txt").exists());
        assert_eq!(
            std::fs::read_to_string(workspace.join("copy.txt")).unwrap(),
            "content"
        );

        let cmd = SafeCommand::parse("mv copy.txt moved.txt").unwrap();
        cmd.execute(workspace).unwrap();
        assert!(!workspace.join("copy.txt").exists());
        assert!(workspace.join("moved.txt").exists());
    }

    #[test]
    fn test_execute_grep() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(
            workspace.join("data.txt"),
            "Hello World\nfoo bar\nHello Again",
        )
        .unwrap();

        let cmd = SafeCommand::parse("grep Hello data.txt").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("Hello World"));
        assert!(output.stdout.contains("Hello Again"));
        assert!(!output.stdout.contains("foo bar"));
    }

    #[test]
    fn test_execute_grep_ignore_case() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        std::fs::write(workspace.join("data.txt"), "HELLO\nhello\nHeLLo").unwrap();

        let cmd = SafeCommand::parse("grep -i hello data.txt").unwrap();
        let output = cmd.execute(workspace).unwrap();

        assert_eq!(output.exit_code, 0);
        let lines: Vec<&str> = output.stdout.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_path_escape_rejected() {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path();

        let cmd = SafeCommand::parse("cat ../../../etc/passwd").unwrap();
        let result = cmd.execute(workspace);

        assert!(result.is_err());
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*.txt", "file.txt"));
        assert!(glob_match("file.*", "file.txt"));
        assert!(glob_match("*.py", "test.py"));
        assert!(!glob_match("*.txt", "file.py"));
    }
}
