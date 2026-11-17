# Shannon 桌面应用

为 Shannon AI agent 构建的多平台桌面应用，基于 [Tauri](https://tauri.app/) 与 [Next.js](https://nextjs.org/)。

## 快速开始

### 前置条件

桌面应用需要连接 Shannon 后端 service。请先启动后端：

```bash
# From repository root
make dev

# Verify services are running
curl http://localhost:8080/health
```

### 方案一：本地 Web UI（开发）

把 UI 作为本地 Web 应用运行，无需构建原生二进制：

```bash
cd desktop

# Install dependencies
npm install

# Start development server
npm run dev

# Open http://localhost:3000
```

**Web 模式下的功能：**
- 实时 SSE event stream
- session 与 task 管理
- 可视化 workflow 执行
- 支持深色模式
- 开发时即时热重载

### 方案二：原生桌面应用

#### 下载预构建二进制

从 [GitHub Releases](https://github.com/Kocoro-lab/Shannon/releases/latest) 下载适用于你平台的最新版本：

| 平台 | 格式 |
|----------|---------|
| **macOS**（Intel 与 Apple Silicon） | `.dmg` 安装包、`.app.tar.gz` |
| **Windows** | `.msi` 安装包、`.exe` NSIS 安装包 |
| **Linux** | `.AppImage`（便携版）、`.deb`（Debian/Ubuntu） |

#### 从源码构建

为你的平台构建原生桌面应用：

```bash
cd desktop

# Install dependencies
npm install

# Build for your platform
npm run tauri:build

# Output locations:
# macOS:   src-tauri/target/universal-apple-darwin/release/bundle/dmg/
# Windows: src-tauri/target/release/bundle/msi/
# Linux:   src-tauri/target/release/bundle/appimage/
```

## Web UI 与原生应用对比

| 功能 | Web UI | 原生应用 |
|---------|--------|------------|
| 快速测试 | 即时（`npm run dev`） | 需要构建 |
| 系统集成 | 有限 | 系统托盘、通知 |
| 离线历史 | 无 | Dexie.js 本地数据库 |
| 性能 | 浏览器开销 | 原生渲染 |
| 文件系统访问 | 有限 | 完整的 Tauri API |
| 自动更新 | 无 | 内置 updater |
| 内存占用 | 较高（浏览器） | 已优化 |

## 开发

### 项目结构

```
desktop/
├── app/              # Next.js app router pages
├── components/       # React components
│   ├── ui/          # shadcn/ui components
│   └── ...          # Custom components
├── lib/             # Utilities and helpers
├── hooks/           # React hooks
├── src-tauri/       # Tauri Rust backend
│   ├── src/        # Rust source code
│   ├── icons/      # App icons
│   └── Cargo.toml  # Rust dependencies
├── public/          # Static assets
└── package.json    # Node dependencies
```

### 可用脚本

```bash
# Development
npm run dev          # Next.js dev server (web mode)
npm run tauri:dev    # Tauri dev mode (native app with hot reload)

# Production
npm run build        # Build Next.js static export
npm run tauri:build  # Build native app for your platform

# Linting
npm run lint         # Run ESLint
```

### 环境配置

为开发创建 `.env.local`：

```bash
# Backend API endpoint
NEXT_PUBLIC_API_URL=http://localhost:8080

# Optional: Enable debug mode
NEXT_PUBLIC_DEBUG=true
```

所有可用选项见 [`.env.local.example`](.env.local.example)。

## 技术栈

| 组件 | 技术 |
|-----------|------------|
| 前端框架 | [Next.js 16](https://nextjs.org/) 配合 App Router |
| UI 组件 | [shadcn/ui](https://ui.shadcn.com/) + [Radix UI](https://www.radix-ui.com/) |
| 样式 | [Tailwind CSS](https://tailwindcss.com/) |
| 桌面 runtime | [Tauri v2](https://tauri.app/) |
| 状态管理 | [Zustand](https://zustand-demo.pmnd.rs/) + [Redux Toolkit](https://redux-toolkit.js.org/) |
| 本地数据库 | [Dexie.js](https://dexie.org/)（IndexedDB 封装） |
| 流程图 | [@xyflow/react](https://reactflow.dev/) |
| Markdown 渲染 | [react-markdown](https://github.com/remarkjs/react-markdown) |

## 构建生产版本

### 前置条件

- **Node.js** 20+
- **Rust**（最新稳定版）—— 从 [rustup.rs](https://rustup.rs/) 安装
- **平台相关依赖**：
  - **macOS**：Xcode Command Line Tools（`xcode-select --install`）
  - **Windows**：Microsoft C++ Build Tools
  - **Linux**：见 [Tauri Prerequisites](https://tauri.app/v2/guides/prerequisites/)

### 构建命令

```bash
# macOS (Universal Binary)
npm run tauri:build -- --target universal-apple-darwin

# Windows
npm run tauri:build -- --target x86_64-pc-windows-msvc

# Linux
npm run tauri:build -- --target x86_64-unknown-linux-gnu

# iOS (macOS only, requires Xcode)
npm run tauri ios build
```

## 自动更新

桌面应用包含自动更新检查：

- **启动时检查**：从 GitHub 查找新版本
- **后台下载**：静默下载更新
- **用户提示**：安装前先征询用户

在 `src-tauri/tauri.conf.json` 中配置更新行为。

## 故障排查

### Web UI 无法启动

```bash
# Clear Next.js cache
rm -rf .next node_modules/.cache
npm install
npm run dev
```

### 连接后端失败

```bash
# Verify backend is running
curl http://localhost:8080/health

# Check API URL in .env.local
cat .env.local | grep NEXT_PUBLIC_API_URL
```

### Tauri 构建失败

```bash
# Update Rust toolchain
rustup update

# Clean build artifacts
cd src-tauri && cargo clean && cd ..
npm run tauri:build
```

### macOS：「App is damaged」错误

应用未使用 Apple Developer 证书签名。可通过以下方式放行：

```bash
# Remove quarantine attribute
xattr -cr /Applications/Shannon.app
```

或者：系统偏好设置 → 隐私与安全性 → 仍要打开

## 参与贡献

开发准则见主仓库的 [CONTRIBUTING.md](../CONTRIBUTING.md)。

桌面端相关的贡献要求：
1. 遵循 `components/` 中现有的组件 pattern
2. 在适用处使用 shadcn/ui 组件
3. 同时测试 Web 模式（`npm run dev`）与原生模式（`npm run tauri:dev`）

## 更多资源

- [Tauri Documentation](https://tauri.app/v2/guides/)
- [Next.js Documentation](https://nextjs.org/docs)
- [Shannon API Documentation](../docs/)
- [Release Process](../RELEASING.md)

## 许可证

MIT License —— 详见 [LICENSE](../LICENSE)。
