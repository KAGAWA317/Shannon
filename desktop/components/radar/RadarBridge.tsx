"use client";

import { useEffect, useRef } from "react";
import { useSelector } from "react-redux";
import { RootState } from "@/lib/store";
import { radarStore } from "@/lib/radar/store";
import { generateAgentColor } from "@/lib/swarm/agent-colors";

// Longer estimate = slower flight to center (more time to see the animation)
// 估计时长越长，飞向中心越慢（动画可见时间更久）
const DEFAULT_ESTIMATE_MS = 45000; // ~45s to center (matches original dashboard)

// Internal system agents that shouldn't appear on the radar visualization
// 内部系统 agent，不应出现在雷达可视化中
const INTERNAL_AGENTS = new Set([
  "orchestrator",
  "planner",
  "title_generator",
  "title-generator",
  "router",
  "decomposer",
  "synthesizer",
  "system",
  "tasklist",
  "workspace",
  "lead",
  "swarm-lead",
  "swarm-supervisor",
]);

// Event types that are metadata/management, not agent activity — skip entirely in radar
// metadata/管理类 event 而非 agent 活动，在雷达中完全跳过
const RADAR_IGNORE_EVENTS = new Set([
  "TASKLIST_UPDATED",
  "TEAM_RECRUITED",
  "WORKSPACE_UPDATED",
  "TASK_STATUS_CHANGED",
]);

// Check if an agent ID is internal/should be hidden
// 判断 agent ID 是否为内部 agent、是否需要隐藏
function isInternalAgent(agentId: string): boolean {
  if (!agentId) return true;
  const normalized = agentId.toLowerCase().trim();
  if (normalized.startsWith("agent-undefined")) return true;
  if (normalized === "undefined" || normalized === "unknown") return true;
  return INTERNAL_AGENTS.has(normalized);
}

/**
 * Bridge component that maps Redux SSE events to the radar store.
 * Renders nothing - just keeps the radar store in sync with Redux events.
 * 将 Redux SSE event 映射到雷达 store 的桥接组件；不渲染任何内容，只负责让雷达 store 与 Redux event 保持同步
 */
export function RadarBridge() {
  const events = useSelector((state: RootState) => state.run.events);
  const status = useSelector((state: RootState) => state.run.status);
  const swarmAgentRegistry = useSelector(
    (state: RootState) => state.run.swarm?.agentRegistry
  );
  const processedRef = useRef<Set<string>>(new Set());
  const tickRef = useRef<number>(0);
  const timeoutRefs = useRef<Map<string, number>>(new Map());
  const initializedRef = useRef<boolean>(false);
  // Track when each flight started (don't reset on subsequent events)
  // 记录每个航班的起始时间（后续 event 不重置）
  const flightStartTimes = useRef<Map<string, number>>(new Map());

  // Track previous status for detecting completion
  // 记录上一次状态，用于检测完成
  const prevStatusRef = useRef<string>(status);

  // Initialize/reset store when status changes to idle or running
  // 状态切换为 idle 或 running 时初始化/重置 store
  useEffect(() => {
    if (status === "idle" || (status === "running" && !initializedRef.current)) {
      // Clear pending timers
      // 清理待执行的定时器
      for (const t of timeoutRefs.current.values()) window.clearTimeout(t);
      timeoutRefs.current.clear();
      processedRef.current.clear();
      flightStartTimes.current.clear();

      // Reset store
      // 重置 store
      radarStore.getState().reset();
      tickRef.current = 0;
      initializedRef.current = status === "running";
    }
  }, [status]);

  // Sync swarm agent colors to radar store
  // 将 swarm agent 配色同步到雷达 store
  useEffect(() => {
    if (!swarmAgentRegistry) {
      radarStore.setState({ agentColors: {} });
      return;
    }
    const colors: Record<string, number> = {};
    for (const [name, info] of Object.entries(swarmAgentRegistry)) {
      colors[name] = generateAgentColor(info.colorIndex).hue;
    }
    radarStore.setState({ agentColors: colors });
  }, [swarmAgentRegistry]);

  // When task completes, accelerate all flights to center
  // task 完成时，让所有航班加速飞向中心
  useEffect(() => {
    const wasRunning = prevStatusRef.current === "running";
    const isNowComplete = status === "completed" || status === "idle";

    if (wasRunning && isNowComplete) {
      // Accelerate all in-progress flights to center
      // 所有进行中的航班加速飞向中心
      const state = radarStore.getState();
      for (const [itemId, item] of Object.entries(state.items)) {
        if (item.status === "in_progress") {
          const tick = ++tickRef.current;
          radarStore.getState().applyTick({
            tick_id: tick,
            items: [{ id: itemId, eta_ms: 300, estimate_ms: 300 }],
          });

          // Remove after animation completes
          // 动画结束后移除
          const handle = window.setTimeout(() => {
            const tick2 = ++tickRef.current;
            radarStore.getState().applyTick({
              tick_id: tick2,
              items: [{ id: itemId, status: "done" }],
              agents_remove: [itemId],
            });
            flightStartTimes.current.delete(itemId);
          }, 500);

          const prev = timeoutRefs.current.get(itemId);
          if (prev) window.clearTimeout(prev);
          timeoutRefs.current.set(itemId, handle);
        }
      }
    }
    prevStatusRef.current = status;
  }, [status]);

  // Process events
  // 处理 event
  useEffect(() => {
    // Only process events for live/running tasks - skip historical sessions
    // This prevents stuck flights at the radar edge for completed sessions
    // 仅处理进行中 task 的实时 event，跳过历史 session；避免已结束 session 在雷达边缘留下卡住的航班
    if (status !== "running") return;

    // Events that spawn or keep a flight active (agent is working)
    // 创建航班或保持航班活跃的 event（agent 正在工作）
    const activeLike = new Set([
      "AGENT_STARTED",
      "AGENT_THINKING",
      "MESSAGE_SENT",
      "TOOL_INVOKED",
      "MESSAGE_RECEIVED",
      "ROLE_ASSIGNED",
      "DELEGATION",
      "DATA_PROCESSING",
      "PROGRESS",
    ]);
    // Events that complete a flight (agent finished this task)
    // 使航班完成的 event（agent 已完成该 task）
    const doneLike = new Set(["AGENT_COMPLETED", "TOOL_COMPLETED"]);

    for (const ev of (events || [])) {
      // LEAD_DECISION: trigger gold pulse at radar center (no flight spawned)
      // LEAD_DECISION：在雷达中心触发金色脉冲（不生成航班）
      if (ev.type === "LEAD_DECISION") {
        const key = ev.stream_id || `${ev.workflow_id}::${ev.seq}::LEAD_DECISION`;
        if (processedRef.current.has(key)) continue;
        processedRef.current.add(key);
        const tick = ++tickRef.current;
        radarStore.getState().applyTick({
          tick_id: tick,
          lead_pulse: true,
        });
        continue;
      }

      // TEAM_RECRUITED activates Lead in radar (gold center ring)
      // TEAM_RECRUITED 会激活雷达中的 Lead（中心金色圆环）
      if (ev.type === "TEAM_RECRUITED") {
        const key = ev.stream_id || `${ev.workflow_id}::${ev.seq}::TEAM_RECRUITED`;
        if (!processedRef.current.has(key)) {
          processedRef.current.add(key);
          const tick = ++tickRef.current;
          radarStore.getState().applyTick({
            tick_id: tick,
            lead_pulse: true,
          });
        }
        continue;
      }

      // Skip non-agent metadata events entirely — they are not agent activity
      // 完全跳过非 agent 的 metadata event——它们不属于 agent 活动
      if (RADAR_IGNORE_EVENTS.has(ev.type)) continue;

      const workflowId = ev.workflow_id || "unknown";
      // Better fallback for agent ID - use tool name for tool events, skip generic fallback
      // 更合理的 agent ID 回退：tool event 使用 tool 名，避免通用回退
      let agentId = ev.agent_id;
      if (!agentId) {
        // For tool events, use the tool name if available
        // tool event 优先使用 tool 名
        if ((ev.type === "TOOL_INVOKED" || ev.type === "TOOL_OBSERVATION") && ev.payload?.tool) {
          agentId = `tool-${ev.payload.tool}`;
        } else {
          // Skip events without agent_id to avoid generic "agent-XXXX" names
          // 跳过没有 agent_id 的 event，避免出现通用的 “agent-XXXX” 名称
          continue;
        }
      }

      // Skip internal system agents - only show user-facing agent activity
      // 跳过内部系统 agent，只展示面向用户的 agent 活动
      if (isInternalAgent(agentId)) continue;

      // One flight per agent - reuse same ID for all events from the same agent
      // 每个 agent 只有一个航班——同一 agent 的所有 event 复用同一 ID
      const id = `${workflowId}::${agentId}`;

      // Deduplicate by event key
      // 按 event key 去重
      const key = ev.stream_id || `${workflowId}::${ev.seq}::${ev.type}::${agentId}`;
      if (processedRef.current.has(key)) continue;
      processedRef.current.add(key);

      // Use current time for flight animation (not event timestamp which may be historical)
      // 航班动画使用当前时间（而非可能是历史值的 event 时间戳）
      const now = Date.now();

      // Active events: spawn new flight OR keep existing one flying
      // 活跃 event：创建新航班或让已有航班继续飞行
      if (activeLike.has(ev.type)) {
        const existing = radarStore.getState().items[id];

        if (!existing) {
          // NEW flight - spawn at edge with CURRENT time (not historical event time)
          // 新航班——以当前时间（而非历史 event 时间）在边缘生成
          const tick = ++tickRef.current;
          flightStartTimes.current.set(id, now);

          radarStore.getState().applyTick({
            tick_id: tick,
            items: [
              {
                id,
                group: "A",
                sector: "PLANNING",
                depends_on: [],
                estimate_ms: DEFAULT_ESTIMATE_MS,
                started_at: now,
                status: "in_progress",
                agent_id: agentId,
                tps_min: 1,
                tps_max: 1,
                tps: 1,
                tokens_done: 0,
                est_tokens: 0,
              },
            ],
            agents: [{ id, work_item_id: id, x: 0, y: 0, v: 0.002, curve_phase: 0 }],
          });
        } else {
          // Agent still active — extend estimate so airplane doesn't arrive at center prematurely.
          // Keep progress capped at ~70%; only AGENT_COMPLETED triggers the final approach.
          // agent 仍在活动——延长估计时长，避免飞机过早抵达中心；进度上限约 70%，只有 AGENT_COMPLETED 才触发最后进近
          const startTime = flightStartTimes.current.get(id) || existing.started_at || now;
          const elapsed = now - startTime;
          const currentEstimate = existing.estimate_ms || DEFAULT_ESTIMATE_MS;
          const minEstimate = elapsed / 0.7; // keeps progress <= 70%
          if (minEstimate > currentEstimate) {
            const tick = ++tickRef.current;
            radarStore.getState().applyTick({
              tick_id: tick,
              items: [{ id, estimate_ms: minEstimate }],
            });
          }
        }
        continue;
      }

      // Completion events: let flight reach center naturally, then pulse and remove
      // 完成 event：让航班自然抵达中心，然后脉冲并移除
      if (doneLike.has(ev.type)) {
        const existing = radarStore.getState().items[id];

        if (existing && existing.status === "in_progress") {
          // Calculate how far the agent has flown
          // 计算 agent 已飞行的距离
          const startTime = flightStartTimes.current.get(id) || existing.started_at || now;
          const elapsed = now - startTime;
          const progress = Math.min(1, elapsed / DEFAULT_ESTIMATE_MS);

          // Accelerate to center: cap at 2s so completed agents don't linger
          // 加速飞向中心：上限 2 秒，避免已完成的 agent 滞留
          const remainingMs = Math.min(2000, Math.max(500, (1 - progress) * DEFAULT_ESTIMATE_MS * 0.3));

          const tick1 = ++tickRef.current;
          radarStore.getState().applyTick({
            tick_id: tick1,
            items: [
              {
                id,
                // Accelerate to center: reduce estimate so progress catches up
                // 加速飞向中心：缩短估计时长以追上进度
                estimate_ms: elapsed + remainingMs,
                eta_ms: remainingMs,
                status: "in_progress",
              },
            ],
          });

          // After the flight completes, mark done and remove
          // 航班完成后标记为完成并移除
          const handle = window.setTimeout(() => {
            const tick2 = ++tickRef.current;
            radarStore.getState().applyTick({
              tick_id: tick2,
              items: [{ id, status: "done" }],
              agents_remove: [id],
            });
            timeoutRefs.current.delete(id);
            flightStartTimes.current.delete(id);
          }, remainingMs + 200); // Extra time for pulse animation

          const prev = timeoutRefs.current.get(id);
          if (prev) window.clearTimeout(prev);
          timeoutRefs.current.set(id, handle);
        }
        continue;
      }

      // WORKFLOW_COMPLETED: complete all remaining flights for this workflow
      // WORKFLOW_COMPLETED：完成该 workflow 剩余的所有航班
      if (ev.type === "WORKFLOW_COMPLETED") {
        const state = radarStore.getState();
        for (const [itemId, item] of Object.entries(state.items)) {
          if (itemId.startsWith(workflowId + "::") && item.status === "in_progress") {
            const tick = ++tickRef.current;
            radarStore.getState().applyTick({
              tick_id: tick,
              items: [{ id: itemId, eta_ms: 300, estimate_ms: 300 }],
            });

            // Remove after animation
            // 动画结束后移除
            const handle = window.setTimeout(() => {
              const tick2 = ++tickRef.current;
              radarStore.getState().applyTick({
                tick_id: tick2,
                items: [{ id: itemId, status: "done" }],
                agents_remove: [itemId],
              });
              flightStartTimes.current.delete(itemId);
            }, 500);
            timeoutRefs.current.set(itemId, handle);
          }
        }
        continue;
      }

      // Error: mark as blocked and remove
      // 错误：标记为阻塞并移除
      if (ev.type === "error") {
        const tick = ++tickRef.current;
        radarStore.getState().applyTick({
          tick_id: tick,
          items: [{ id, status: "blocked" }],
          agents_remove: [id],
        });
        flightStartTimes.current.delete(id);
        continue;
      }

      // Fallback: spawn flight for any other event if not seen
      // 兜底：其他未见过的 event 也生成航班
      const existing = radarStore.getState().items[id];
      if (!existing) {
        const tick = ++tickRef.current;
        flightStartTimes.current.set(id, now);

        radarStore.getState().applyTick({
          tick_id: tick,
          items: [
            {
              id,
              group: "A",
              sector: "PLANNING",
              depends_on: [],
              estimate_ms: DEFAULT_ESTIMATE_MS,
              started_at: now,
              status: "in_progress",
              agent_id: agentId,
              tps_min: 1,
              tps_max: 1,
              tps: 1,
              tokens_done: 0,
              est_tokens: 0,
            },
          ],
          agents: [{ id, work_item_id: id, x: 0, y: 0, v: 0.002, curve_phase: 0 }],
        });
      }
    }
  }, [events, status]);

  // Cleanup on unmount
  // 卸载时清理
  useEffect(() => {
    const currentTimeouts = timeoutRefs.current;
    return () => {
      for (const t of currentTimeouts.values()) window.clearTimeout(t);
      currentTimeouts.clear();
    };
  }, []);

  return null;
}
