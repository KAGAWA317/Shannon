/**
 * Utility functions for filtering status/diagnostic messages
 * Used to determine if a message should be displayed in conversation vs timeline-only
 *
 * 过滤状态/诊断类消息的工具函数。
 * 用于判断一条消息应展示在对话中，还是只展示在 timeline 中。
 */

/**
 * Check if a message is a short status/completion message that should be filtered
 * from conversation display (these appear in timeline only)
 * 判断消息是否为应被过滤掉的简短状态/完成消息
 * （这类消息只在 timeline 中展示，不出现在对话里）
 *
 * @param content - The message content to check
 * @returns true if the message is a status message that should be filtered
 */
export function isStatusMessage(content: unknown): boolean {
    if (!content || typeof content !== 'string') return false;

    const trimmed = content.trim();
    const lower = trimmed.toLowerCase();

    // Only filter short messages (< 100 chars) to avoid dropping real content
    // 只过滤 100 字符以内的短消息，避免把真实内容误删
    if (trimmed.length >= 100) return false;

    // Exact matches
    if (lower === 'done' || lower === 'completed' || lower === 'success' || lower === 'all done') {
        return true;
    }

    // Pattern matches for short status messages
    if (/^(task\s+)?(completed|done|success)\.?$/i.test(lower)) {
        return true;
    }

    // Status phrases
    if (lower.includes('task completed') ||
        lower.includes('task done') ||
        lower.includes('workflow completed') ||
        lower.includes('successfully completed') ||
        lower.includes('successfully')) {
        return true;
    }

    return false;
}

/**
 * Check if a message is a diagnostic/system message that should always be filtered
 * from conversation display regardless of length
 * 判断消息是否为诊断/系统消息；这类消息无论长短都应从对话中过滤掉
 *
 * @param content - The message content to check
 * @returns true if the message is a diagnostic message that should be filtered
 */
export function isDiagnosticMessage(content: unknown): boolean {
    if (!content || typeof content !== 'string') return false;

    return content.startsWith('[Incomplete response:') ||
           content.includes('Task budget at');
}

/**
 * Combined check: should this message be filtered from conversation?
 * Returns true for both status and diagnostic messages
 * 组合判断：这条消息是否应从对话中过滤？
 * 状态消息与诊断消息都返回 true。
 */
export function shouldFilterFromConversation(content: unknown): boolean {
    return isDiagnosticMessage(content) || isStatusMessage(content);
}

/**
 * Maximum number of processed delta sequences to track per message
 * Prevents unbounded memory growth during long streaming sessions
 * 每条消息最多跟踪的已处理 delta 序列数。
 * 防止长时间 stream session 中内存无限增长。
 */
export const MAX_PROCESSED_DELTAS = 100;

/**
 * Cap an array of processed delta sequences to prevent memory growth
 * Keeps the most recent entries
 * 截断已处理 delta 序列数组以防止内存增长，保留最近的条目。
 */
export function capProcessedDeltas(deltas: number[], newSeq?: number): number[] {
    const result = newSeq !== undefined ? [...deltas, newSeq] : deltas;
    if (result.length > MAX_PROCESSED_DELTAS) {
        return result.slice(-MAX_PROCESSED_DELTAS);
    }
    return result;
}
