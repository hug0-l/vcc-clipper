# Changelog

All notable changes to Clipper are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### 🐛 修復

- **管理面板無法取得伺服器日誌** — `_refreshLogViewer()` 現在會向伺服器請求日誌，切換管理頁籤時自動刷新
- **伺服器設定未回傳到網頁端** — 登入成功時一併回傳 config（含 logDir），管理面板正確顯示日誌路徑
- **`secrets` 模組未 import** — 導致 admin session token 生成時 `NameError`
- **JS 殘留孤立程式碼** — 造成整個 script 無法解析，web client freeze
- **檔案傳輸無法選取 relay-only 對等點** — 新增 `isPeerReachable()` 輔助函式，WS Relay 用戶現在可被選取為傳送對象
- **管理面板 unauthorized 未處理** — 會話過期時自動回到登入畫面
- **STUN 變更未即時生效** — 儲存 STUN 伺服器後立即更新本地狀態
- **CSS 孤立 @keyframes block** — 移除殘留的無效 CSS

### 🔧 改善

- **管理面板服務器日誌路徑** — 正確顯示 `logs/clipper_&lt;date&gt;.log` 而非資料庫路徑
- **檔案傳輸單一對等點自動選取** — 只剩一位可送達對象時自動選取，減少操作步驟
- **對等點離開時清理已選取對象** — `removePeer()` 現在一併清理 `selectedTargetPeerIds`
- **管理面板切換自動刷新** — 切換到日誌/設定頁籤時自動請求最新資料
- **`admin-set-config` 回應含 config** — 伺服器在設定更新後回傳當前 config 狀態
- **NTP 伺服器驗證** — `_ntp_query()` 回傳 `(offset, is_valid)` tuple，區分「偏移為零」與「查詢失敗」
- **NTP 驗證可視化** — 管理面板偏移量以 🟢 綠色（正常）/ 🔴 紅色（無回應）顯示，hover 可查看 tooltip
- **NTP 儲存時即時驗證** — 儲存 NTP 伺服器後即顯示連線成功或失敗的明確訊息
- **內建 HTTP Server** — 伺服器自動在 port 8766 提供 `clipper.html` 及靜態檔案，無需手動開啟檔案
- **WebSocket 重連遺失顯示名稱** — `autoReconnect()`、toast 重連按鈕、`generated` 處理器現在傳送 `displayName`，避免 peer-list 顯示 peerId 而非名稱
- **顯示名稱變更未通知伺服器** — 內嵌編輯顯示名稱時自動發送 `register-name`，peer-list 即時更新
- **線上用戶列表改善** — 顯示 peerId 在名稱旁方便辨識；`isSelf` 判斷改為純 peerId 比對
- **程式碼審查修復** — 修復 5 個潛在 bug（dead code、重複 WS 連線、`setChecklistReminder` 未定義變數、重複 debug dump 監聽器、case 格式錯誤）

## [1.1.0] — 2026-06-19

### 🚀 新增功能

- **📋 公告欄分頁** — 完整 CRUD 公告貼文，支援釘選置頂
- **🏷️ 公告欄分類** — 4 級分類（重要🔴/日常🔵/交接事項🟡/其他事項⚪），自動色碼、標籤 badges
- **✅ 檢查清單 Boards** — 階層式結構，每個 Board 可獨立命名、設色、加標籤、釘選、可收合
- **🔄 Tick Reset** — 一鍵重設 Board 內所有勾選
- **🔔 排程提醒** — 設定日期時間，到期自動彈窗通知
- **💬 實時短信持久化** — 聊天訊息自動儲存至 localStorage，重整不消失（最多 200 則）
- **🎨 用戶顏色區分** — 根據顯示名稱自動分配 HSL 色相，一眼辨識發話者
- **🗑️ 清除本機聊天紀錄** — 確認對話框防誤刪
- **🖥️ 伺服器持久化** — 公告欄、檢查清單、聊天備份全部儲存至伺服器 SQLite 資料庫
- **🔧 聊天備份留存** — 可設定 `CHAT_RETENTION_DAYS`（預設 7 天）
- **🔄 WS Relay 後備** — WebRTC DataChannel 失敗時自動降級為 WebSocket 中繼傳輸
- **⚙️ 設定分頁** — 檢視傳輸模式（P2P/Relay）、各用戶連線狀態、STUN 伺服器設定
- **🌐 STUN 支援** — 預設 Google STUN 伺服器，設定頁可自訂位址
- **📤 並行檔案傳輸** — 各對象獨立傳送不阻塞，A 傳 B 同時 C 可傳 D
- **🔍 Debug Dump** — 一鍵輸出伺服器完整診斷資訊
- **🕐 頁頂時鐘** — 即時顯示 hh:mm:ss 及當日日期
- **🔗 預設配對碼** — 自動填入 1234 並在頁面載入後快速建立配對
- **✏️ 顯示名稱跨 F5 保留** — 使用 localStorage 儲存，重整不重置
- **🔔 Popup 通知** — 新公告/新檢查清單/新待辦項目時右下角彈窗
- **🟢 同步狀態指示器** — footer 顯示最後同步時間 + 手動同步按鈕

### 🔧 改善

- **📱 自適應佈局** — 視窗縮小時排版自動折疊、不隱藏任何文字
- **🔤 字體放大 25%** — 全介面字體等比放大，提升可讀性
- **🛡️ Room-State Merge** — 同步時合併而非覆寫，保留本機資料
- **🚨 連線守衛** — 公告編輯等操作補上連線檢查
- **🗑️ 防誤刪確認** — 公告欄及檢查清單所有刪除動作皆有確認對話框
- **📂 檔案傳輸改為需選對象** — 不再預設傳送給所有人
- **📤 檔案傳輸並行化** — `fileSending` 改為 `Map<peerId, entry>`，各對象不互相阻塞
- **🔊 伺服器 Verbose DEBUG** — `_debug()` 微秒精度日誌，所有 WS 收發可追蹤
- **🔄 自動重新同步** — 斷線重連後自動發送 state-get
- **🌐 已知限制更新** — 4 項限制各有解決方案或已實作

### 🐛 修復

- **資料覆蓋問題** — room-state 改為合併而非覆寫 local-only 資料

### 🏗️ 架構

- **C/S 分離** — WebSocket 負責持久資料 CRUD + Relay，DataChannel 負責即時通訊
- **localStorage + 伺服器 JSON 雙重持久化**
- **聊天備份** — 7 日留存可調，不影響 P2P 即時通訊
- **WebRTC 優先，WS Relay 後備** — 自適應傳輸模式
- **FileSending per-peer** — 各對象獨立傳送佇列

### 檔案統計 (vs v1.0)

| 檔案 | v1.0 | 1.1.0 | 變更 |
|------|------|-------|------|
| clipper.html | 1,497 行 | ~4,550 行 | +3,053 |
| signal_server.py | 190 行 | ~1,200 行 | +1,010 |
| CHANGELOG.md | — | 新增 | +85 |
| README.md | 99 行 | 170 行 | +71 |

### 完整 Commit History

```
b183e3a docs: update about page, README and CHANGELOG for v1.1.0
1f334a4 feat(relay): add WebSocket relay fallback + settings tab
c7d406d feat(server): add verbose DEBUG logging
...共 30+ 個 commits
```

## [1.0.0] — 初始版本

- 內網跨子網 P2P 多人協作工具
- WebRTC Full Mesh 連線
- 聊天室即時訊息廣播
- 檔案傳輸（拖放上傳、大檔案區塊傳輸、指定對象）
- 隨機中文顯示名稱系統
- WebSocket 信令伺服器
