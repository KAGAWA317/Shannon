// Prevents additional console window on Windows in release, DO NOT REMOVE!!
// 在 Windows 的 release 构建中避免额外弹出控制台窗口，请勿删除！！
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
  app_lib::run();
}
