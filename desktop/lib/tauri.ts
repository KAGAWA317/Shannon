// Tauri stub for OSS web-only mode (no desktop app)
// OSS 纯 Web 模式下的 Tauri stub（无桌面端）

export function isTauri(): boolean {
    return false;
}

export async function saveFileDialog(
    _contentOrOptions?: any,
    _fileName?: string,
    _filters?: { name: string; extensions: string[] }[]
): Promise<string | null> {
    return null;
}

export async function sendNotification(_title: string, _body?: string): Promise<void> {
    // No-op in web-only mode
}

export async function setBadgeCount(_count: number): Promise<void> {
    // No-op in web-only mode
}
