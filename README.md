# Clipper — 內網跨子網 P2P 多人協作工具 / Intranet P2P Collaboration Tool

**v2.2.0** — [中文] 內網 P2P 多人協作工具 (WebRTC + WebSocket)。即時聊天、檔案傳輸、公告欄、檢查清單、密鑰管理、**插件系統**、i18n 中英文切換。附 JS SDK、REST API、WebSocket 協定文件。離線唯讀防護、幽靈復活防護。適用於廣播電視台內網協作。

**[EN] Intranet P2P collaboration tool (WebRTC + WebSocket). Real-time chat, file transfer, notice board, checklists, key management, **plugin system**, i18n bilingual support. Includes JS SDK, REST API, WebSocket protocol docs. Offline read-only protection, ghost resurrection prevention.

![version](https://img.shields.io/badge/version-2.2.0-blue)
![python](https://img.shields.io/badge/python-3.10%2B-green)
![build](https://img.shields.io/github/actions/workflow/status/hug0-l/Clipper/.github/workflows/build.yml?branch=main&amp;label=build&amp;logo=github)
![release](https://img.shields.io/github/v/release/hug0-l/Clipper)
![license](https://img.shields.io/badge/license-MIT-lightgrey)

## 快速開始 / Quick Start

### 🐍 Python 啟動 / Python Launch

1. **安裝依賴 / Install dependencies:**
   ```bash
   pip install websockets
   ```

2. **啟動伺服器 / Start server:**
   ```bash
   python3 signal_server.py
   ```
   啟動後 / After starting:
   - **WebSocket**: `ws://localhost:8765`
   - **HTTP**: `http://localhost:8766`（瀏覽器開啟 / open in browser ✅）

3. **自動連線 / Auto-connect:**
   頁面載入後自動使用配對碼 **1234** 建立房間。
   Page loads and auto-connects with default code **1234**.

### 📦 一鍵啟動 / One-Click Launch (PyInstaller)

```bash
# 下載 Release 執行檔 / Download from Releases
# Windows: clipper-server.exe（雙擊 / double-click）
# macOS: clipper-server（終端）或 Clipper.app（雙擊 / double-click）

# 啟動後自動提供 / After launch:
# - WebSocket: ws://localhost:8765
# - HTTP: http://localhost:8766
# - 預設配對碼 / Default code: 1234
```

### 🤖 GitHub Actions 自動編譯 / Auto Build

推送 `v*` tag 時自動編譯跨平台執行檔並建立 Release。
Auto-builds cross-platform binaries on `v*` tag push.

| Platform | Artifact | Trigger |
|----------|----------|---------|
| 🪟 Windows | `clipper-server.exe` | `git tag v2.2.0 && git push` |
| 🍎 macOS | `clipper-server` + `Clipper.app.zip` | Same |


## 🚀 功能 / Features

> [中文] 即時協作功能，支援多人同時編輯與通訊。
> [EN] Real-time collaboration features with multi-user support.

### 💬 實時短信 / Chat
- 即時 P2P 訊息廣播，自動顏色區分不同用戶
- 訊息持久化 — F5 重整不消失（localStorage 最多 200 則）
- 伺服器備份 — 可設定保留天數（預設 7 天）
- 「🗑 清除本機紀錄」按鈕（含確認對話框）
- WebRTC DC 失敗時自動降為 WS Relay 中繼

### 📁 檔案傳輸 / File Transfer
- 拖放上傳，支援大檔案區塊傳輸與進度條
- 指定對象傳送（需手動選取傳送對象）
- 錯誤狀態顯示與重試功能

### 📋 公告欄 / Notice Board
- 完整的公告 CRUD（新增 / 編輯 / 刪除 / 釘選）
- 4 級分類：**重要🔴** · **日常🔵** · **交接事項🟡** · **其他事項⚪**
- 標籤 badges、自動色碼
- 「重要」公告刪除有額外安全確認
- 經 WebSocket 同步至所有用戶

### ✅ 檢查清單 / Checklist
- 階層式 Board 結構，每個 Board 可獨立命名、設色、加標籤
- 10 色色盤（天藍/綠/紅/琥珀/紫/粉/青/橙/灰/白）
- 待辦項目新增 / 勾選 / 刪除
- **🔄 Tick Reset** — 一鍵重設所有勾選
- **🔔 排程提醒** — 設定日期時間，到期自動彈窗通知
- Board 級釘選、可收合
- 經 WebSocket 同步至所有用戶

### ⚙️ 設定頁 / Settings
- 即時檢視傳輸模式（🟢 P2P / 🟡 WS Relay）
- 各用戶連線狀態列表
- 最後同步時間、房間代碼

### 📦 clipper-sdk.js — 外部應用 3 行接入 (v1.2+)
- 輕量 JavaScript SDK (1,317行)，無外部依賴
- 支援所有 Clipper 協議 message types
- **WebRTC P2P 優先** — RTCPeerConnection + DataChannel，失敗自動降級 WS Relay
- 事件驅動 API：`on('connected'|'chat'|'file-meta'|'file-done'|...)`
- 聊天 + 公告 + 檢查清單 + 密鑰管理 + 檔案傳輸（含佇列 + 重試）
- SHA-256 完整性驗證

### 🛡️ 離線唯讀 / Offline Read-only (v1.1.1+)
- **斷線自動唯讀** — 信令伺服器中斷時自動進入唯讀模式，所有協作功能（聊天、檔案、公告、檢查清單、密鑰管理）鎖定
- **重連自動解除** — 重連成功後自動恢復完整功能
- **視覺提示** — 頂端琥珀色橫幅 + 按鈕灰色半透明 + 檔案區遮罩，使用者一目瞭然
- **22 函式攔截** — 所有可能修改資料的操作皆有唯讀檢查，無死角

### 🧟 幽靈復活防護 / Ghost Prevention (v1.1.1+)
- 伺服器記錄所有已刪除的 IDs（公告、檢查清單、密鑰）
- 重連時 room-state 合併自動過濾被刪項目
- 向後相容：舊版伺服器不發 `deletedIds` 則跳過過濾

### 🖥️ 伺服器 / Server
- 公告欄、檢查清單、聊天備份全資料持久化（SQLite + JSON 後備）
- `clipper_data.db` (SQLite) 自動存檔／載入，支援 WAL 模式
- 可設定聊天備份留存天數（`CHAT_RETENTION_DAYS`）
- **WS Relay** — WebRTC 失敗時自動轉為伺服器中繼
- **Debug Dump** — 用戶端一鍵輸出完整診斷資訊
- Verbose DEBUG 模式（`DEBUG = True`）
- **自動跳轉瀏覽器** — 啟動後自動開啟 `http://localhost:8766`

### 👥 網路 / Networking
- Full Mesh WebRTC 網狀架構，最多 50 台同時連線
- WebSocket 中繼後備方案（WebRTC 失敗時自動切換）
- 新成員加入自動與所有人配對
- 斷線自動清理，可重新連線

### 🔌 插件系統 / Plugin System (v2.2.0+)
- 動態載入 Client 插件：新增分頁、註冊 WS handlers、自訂 CSS
- Server 插件：`@register()` 裝飾器自動註冊 WS 訊息類型
- 插件專用持久化儲存（`plugin_set` / `plugin_get` / `plugin_list`）
- 管理頁 GUI 啟用/停用/移除插件
- 支援從 URL 或本機檔案載入插件

### 🏷️ 顯示名稱 / Display Name
- 隨機中文暱稱（形容詞 + 動物 + 數字，如「快樂熊貓42」）
- 可點擊編輯自訂名稱
- 跨 F5 保留（localStorage `vcc_display_name`）

### 🔍 其他功能 / Other Features
- 🕐 頁頂即時時鐘（hh:mm:ss + 當日日期）
- 🔗 預設配對碼 1234，開啟頁面即自動連線
- 🟢 同步狀態指示器 + 手動同步按鈕
- 📱 自適應佈局（視窗縮小時排版自動折疊）
- 🔤 全介面字體放大 25% 提升可讀性
- 🔔 Popup 通知 — 新公告/新檢查清單時右下角彈窗
- 所有刪除動作皆有確認對話框防誤刪
- Room-State Merge — 同步時合併而非覆寫本機資料
- 🎨 淺色/深色主題切換（`Ctrl+Shift+E`）
- ⌨️ 快捷鍵：`Ctrl+F` 搜尋、`Ctrl+K` 切換分頁、`Esc` 關閉彈窗
- 🌙 PWA 支援：可加入主畫面（manifest.json + Service Worker）

## 專案架構 / Architecture

```
├── clipper.html              # 主用戶端 SPA（WebRTC + WS + 所有功能）
├── signal_server.py          # 信令伺服器入口（WebSocket 配對 + HTTP）
├── services/                 # 伺服器邏輯模組
│   ├── ws_router.py          #   WS 訊息路由（@register 裝飾器模式）
│   ├── persistence.py        #   SQLite 持久化層
│   ├── room_service.py       #   房間管理 + peer 追蹤
│   ├── chat_service.py       #   聊天備份／編輯
│   ├── notice_service.py     #   公告欄 CRUD
│   ├── checklist_service.py  #   檢查清單 CRUD（含拖曳排序）
│   ├── keymgmt_service.py    #   密鑰管理 CRUD
│   └── plugin_loader.py      #   Server 插件載入器
├── js/                       # 前端 JavaScript 模組
│   ├── core/                 #   核心基礎設施
│   │   ├── message-bus.js    #    事件匯流排
│   │   ├── ws-manager.js     #    WebSocket 管理器
│   │   ├── module-base.js    #    模組基類
│   │   ├── plugin-registry.js#    插件註冊與管理
│   │   └── plugin-loader.js  #    動態插件載入
│   └── modules/              #   功能模組
│       ├── chat-module.js    #    聊天
│       ├── files-module.js   #    檔案傳輸
│       ├── notice-module.js  #    公告欄
│       ├── checklist-module.js#   檢查清單
│       ├── keymgmt-module.js #    密鑰管理
│       ├── admin-module.js   #    管理面板
│       ├── diagnostic-module.js # 診斷
│       └── templates-module.js #  範本庫
├── plugins/                  # Client 插件存放目錄
│   ├── manifest.json         #   插件清單
│   └── counter-plugin.js     #   範例插件（協作計數器）
├── server_plugins/           # Server 插件存放目錄
│   └── echo_plugin.py        #   範例 server 插件
├── PLUGINS.md                # 插件開發文件
├── AGENTS.md                 # AI 輔助開發指引
├── CHANGELOG.md              # 版本變更記錄
├── README.md                 # 本文件
├── manifest.json             # PWA manifest
├── sw.js                     # Service Worker
├── clipper_data.db           # 伺服器自動產生的 SQLite 持久化資料庫
└── logs/                     # 伺服器自動產生的日誌檔案（24h 輪替）
```

### 通訊協議 / Protocol

| Layer | Transport | Purpose |
|-------|-----------|---------|
| Signaling | WebSocket (WS) | Room pairing, WebRTC offer/answer/ICE, relay data, admin |
| Real-time | WebRTC DataChannel | Chat, file transfer (P2P, auto-fallback to Relay) |
| Fallback | WS Relay | When DC fails, messages relay through server |
| Static | HTTP (:8766) | Serves clipper.html + static files |

## 手動 E2E 測試流程

### 前置準備

1. 啟動信令伺服器：`python3 signal_server.py`
2. 確認終端機顯示 `listening on ws://localhost:8765`
3. 在多個瀏覽器分頁中開啟 `clipper.html`（或 `http://localhost:8765` 透過 HTTP 提供）1. 啟動信令伺服器：`python3 signal_server.py`
2. 確認終端機顯示 `listening on ws://localhost:8765  |  http://localhost:8766`
3. 在瀏覽器中開啟 `http://localhost:8766`（或多個分頁）### 測試步驟

| 步驟 | 動作 | 預期結果 |
|------|------|----------|
| **1. 配對連線** | 開啟兩個分頁，自動使用 1234 配對 | 兩端狀態顯示「🟢 已連線」 |
| **1b. 自動同步** | 在分頁 A 新增公告 | 分頁 B 自動出現該公告 |
| **2. 實時短信** | 分頁 A 輸入文字後按 Enter | 分頁 B 收到訊息，顯示不同顏色 |
| **2b. 顯示名稱** | 點擊頂部的顯示名稱 → 編輯新名稱 → 按 Enter | 下一則訊息以新名稱顯示，F5 後仍保留 |
| **3. 檔案傳送** | 分頁 A 選取對象後拖放檔案到傳輸區 | 分頁 B 自動下載該檔案 |
| **4. 公告欄** | 分頁 A 新增公告 + 選擇分類 + 輸入標籤 | 分頁 B 顯示分類色碼與標籤 badges |
| **5. 檢查清單** | 分頁 A 新增 Board + 新增待辦項目 | 分頁 B 顯示 Board 與項目 |
| **5b. 排程提醒** | 設定提醒時間 | 時間到時雙方彈窗通知 |
| **5c. Tick Reset** | 點 🔄 按鈕 | 所有勾選重設為未完成 |
| **6. 同步狀態** | 檢視 footer 同步指示器 | 顯示 🟢 剛剛同步 |
| **7. Debug Dump** | 在關於頁點 Debug Dump | 主控台輸出完整伺服器診斷 |
| **8. 中斷連線** | 關閉伺服器後重新啟動 | 客戶端顯示「🔴 未同步」，重連後自動合併 |
| **9. 傳輸模式** | 檢視設定頁 | 顯示 🟢 P2P 或 🟡 Relay 模式 |

## 設定 / Configuration

### 伺服器 / Server (signal_server.py)

```python
CHAT_RETENTION_DAYS = 7    # 聊天備份留存天數 / Chat message retention (days)
DB_PATH = "clipper_data.db"   # SQLite 資料庫路徑 / Database path
LOG_DIR = "logs"               # 日誌目錄 / Log directory
DEBUG = True                 # Verbose debug logs
```

### 用戶端 / Client (clipper.html)

- 配對碼預設 / Default pairing code: `1234`
- 伺服器 URL 預設 / Default server URL: `ws://localhost:8765`

## 已知限制 / Known Limitations

| 限制 / Limitation | 狀態 / Status | 備註 / Note |
|------|---------|----------|
| 跨子網 UDP 穿透 / Cross-subnet UDP | ✅ STUN 內建 + WS Relay 備援 | `stun:stun.l.google.com:19302` |
| 多人同時傳檔阻塞 / Parallel file transfer | ✅ 獨立佇列 / Per-peer queues | A 傳 B 不影響 C 傳 D |
| 大檔案 WS Relay overhead | ⚠️ 僅限 P2P | <10MB relay ok, larger → P2P |
| 房間上限 / Room limit | `MAX_PEERS_PER_ROOM` | 建議 / Recommend ≤ 20 |
| Python 3.8+ 需求 | ✅ PyInstaller 已打包 | 不需安裝 Python / No Python needed |

## 授權

MIT License — 詳見專案授權文件。
