# Changelog

All notable changes to VCC Clipper are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.1.0] — 2026-06-19

### 🚀 新增功能

- **📋 公告欄分頁** — 完整 CRUD 公告貼文，支援釘選置頂
- **✅ 檢查清單分頁** — 新增/勾選/刪除待辦事項
- **🏷️ 公告欄分類** — 4 級分類（重要🔴 / 日常🔵 / 交接事項🟡 / 其他事項⚪），自動色碼、標籤 badges
- **📋 檢查清單 Boards** — 階層式結構，每個 Board 可獨立命名、設色、加標籤、釘選
- **🔄 Tick Reset** — 一鍵重設 Board 內所有勾選
- **💬 實時短信持久化** — 聊天訊息自動儲存至 localStorage，重整不消失
- **🗑️ 清除本機聊天紀錄** — 確認對話框防誤刪
- **🖥️ 伺服器持久化** — 公告欄、檢查清單、聊天備份全部儲存至伺服器 JSON 檔案
- **🔍 Debug Dump** — 一鍵輸出伺服器完整診斷資料
- **🕐 頁頂時鐘** — 即時顯示 hh:mm:ss 及當日日期
- **🔗 預設配對碼** — 自動填入 1234 並在頁面載入後快速建立配對
- **✏️ 顯示名稱標示** — 輸入名稱旁顯示「你現在顯示為：」

### 🔧 改善

- **📱 自適應佈局** — 視窗縮小時排版自動折疊、不隱藏任何文字
- **🔤 字體放大 25%** — 全介面字體等比放大，提升可讀性
- **🛡️ Room-State Merge** — 同步時合併而非覆寫，保留本機資料
- **🚨 連線守衛** — 公告編輯等操作補上連線檢查
- **🗑️ 防誤刪確認** — 公告欄及檢查清單所有刪除動作皆有確認對話框

### 🏗️ 架構

- **C/S 分離** — WebSocket 負責持久資料 CRUD，DataChannel 負責即時通訊
- **localStorage + 伺服器 JSON 雙重持久化**
- **聊天備份** — 7 日留存可調，不影響 P2P 即時通訊

### 檔案統計 (vs v1.0)

| 檔案 | v1.0 | 1.1.0 | 變更 |
|------|------|-------|------|
| VCC_Clipper.html | 1,497 行 | ~2,630 行 | +1,133 |
| signal_server.py | 190 行 | ~570 行 | +380 |

## [1.0.0] — 初始版本

- 內網跨子網 P2P 多人協作工具
- WebRTC Full Mesh 連線
- 聊天室即時訊息廣播
- 檔案傳輸（拖放上傳、大檔案區塊傳輸、指定對象）
- 隨機中文顯示名稱系統
- WebSocket 信令伺服器
