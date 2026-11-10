/**
 * Agent color system for swarm visualization.
 *
 * - Lead agent: fixed amber (gold)
 * - Worker agents: HSL golden-angle rotation for maximum visual separation
 * - Colors scoped per workflow run (reset on new swarm task)
 *
 * swarm 可视化用的 agent 配色系统。
 * - Lead agent：固定琥珀色（金色）
 * - Worker agent：按黄金角旋转 HSL 色相，获得最大的视觉区分度
 * - 颜色按 workflow run 作用域隔离（新 swarm task 时重置）
 */

export interface AgentColor {
  /** CSS hsl() for backgrounds with alpha */
  bg: string;
  /** CSS hsl() for text */
  text: string;
  /** CSS hsl() for borders with alpha */
  border: string;
  /** CSS hsl() for status dots */
  dot: string;
  /** Raw hue value (for canvas/programmatic use) */
  hue: number;
}

/** Fixed amber color for Lead agent */
export const LEAD_COLOR: AgentColor = {
  bg: "hsl(38 92% 50% / 0.15)",
  text: "hsl(38 92% 65%)",
  border: "hsl(38 92% 50% / 0.3)",
  dot: "hsl(38 92% 55%)",
  hue: 38,
};

/**
 * Generate a unique agent color by index using the golden angle.
 * Golden angle (137.508deg) guarantees maximum hue separation for any N.
 * Offset avoids red (hue 0) for early indices; red zone skip prevents
 * any index from landing on red which looks like an error state.
 *
 * 按索引使用黄金角生成互不相同的 agent 颜色。
 * 黄金角（137.508 度）能保证任意 N 下色相间隔最大。
 * 偏移量让靠前的索引避开红色（色相 0）；跳过红色区则避免
 * 任何索引落在红色上，红色在 UI 中看起来像错误状态。
 */
const HUE_OFFSET = 160; // Start from teal-green, not red
// 从青绿色开始，避免初始索引落在红色上

export function generateAgentColor(index: number): AgentColor {
  let hue = Math.round((HUE_OFFSET + index * 137.508) % 360);
  // Skip red zone (345°-15°) — red resembles error/failure in UI
  // 跳过红色区（345°-15°）—— 红色在 UI 中容易被理解为错误/失败
  if (hue > 345 || hue < 15) {
    hue = (hue + 30) % 360;
  }
  return {
    bg: `hsl(${hue} 70% 50% / 0.15)`,
    text: `hsl(${hue} 70% 65%)`,
    border: `hsl(${hue} 70% 50% / 0.3)`,
    dot: `hsl(${hue} 70% 55%)`,
    hue,
  };
}

/**
 * Agent color registry — maps agent names to colors within a workflow run.
 * Call `getOrAssign()` when an agent first appears (AGENT_STARTED event).
 * Call `reset()` when a new swarm workflow starts.
 *
 * agent 颜色注册表 —— 在单个 workflow run 内把 agent 名称映射到颜色。
 * agent 首次出现（AGENT_STARTED event）时调用 `getOrAssign()`。
 * 新的 swarm workflow 开始时调用 `reset()`。
 */
export class AgentColorRegistry {
  private map = new Map<string, AgentColor>();
  private nextIndex = 0;

  /** Get existing color or assign the next one. Lead always gets LEAD_COLOR. */
  /** 取已有颜色，没有则分配下一个；Lead 始终使用 LEAD_COLOR。 */
  getOrAssign(agentId: string): AgentColor {
    if (agentId === "swarm-lead" || agentId === "swarm-supervisor") {
      return LEAD_COLOR;
    }
    const existing = this.map.get(agentId);
    if (existing) return existing;
    const color = generateAgentColor(this.nextIndex++);
    this.map.set(agentId, color);
    return color;
  }

  /** Look up color without assigning. Returns undefined if agent unknown. */
  get(agentId: string): AgentColor | undefined {
    if (agentId === "swarm-lead" || agentId === "swarm-supervisor") {
      return LEAD_COLOR;
    }
    return this.map.get(agentId);
  }

  /** Reset for a new workflow run. */
  reset(): void {
    this.map.clear();
    this.nextIndex = 0;
  }

  /** Get all registered agents and their colors. */
  entries(): [string, AgentColor][] {
    return Array.from(this.map.entries());
  }
}
