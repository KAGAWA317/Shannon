# Providers 与路由

## GPT‑5 系列路由

- GPT‑5 模型被路由到 OpenAI Responses API。
- 存在 `output_text` 时优先使用它，以免 API 返回结构化块时内容为空。
- 在 synthesis 阶段使用较低的 reasoning effort，以促使产出最终文本。

原因：
- 部分 GPT‑5 chat 响应会以结构化分片的形式返回内容；使用 Responses API 可避免 `message.content` 为空。

## Chat 内容归一化（纵深防御）

- 对于 Chat Completions（OpenAI 及 OpenAI‑compatible），若 `message.content` 是列表，则从每个分片中提取 `.text` 并拼接。
- 这既提供向后兼容，也能抵御 provider 之间的差异。

## Tool 门控语义

- LLM 服务在 `/agent/query` 中期望收到 `allowed_tools`。
- 语义：
  - 省略该字段 → role 预设可以启用 tools。
  - `[]` → 禁用 tools。
  - `["name", …]` → 只有这些 tools 可用（built‑in、OpenAPI，或按注册名指定的 MCP）。

