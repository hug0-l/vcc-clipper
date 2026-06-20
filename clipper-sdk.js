/**
 * ClipperSDK — 輕量級 Clipper WebSocket 協議 JS SDK
 *
 * 無外部依賴，單一檔案，約 450 行。
 *
 * 使用方式：
 *   const clipper = new ClipperSDK({ server: 'ws://localhost:8765', room: '1234', displayName: '計時器' });
 *   clipper.on('connected', () => {});
 *   clipper.connect();
 *
 * 事件列表：
 *   connected, disconnected, chat, notice, peer-joined, peer-left,
 *   state, error, readonly
 */
(function (global) {
  'use strict';

  // ========== EventEmitter ==========

  class EventEmitter {
    constructor() {
      this._events = {};
    }

    on(event, fn) {
      (this._events[event] || (this._events[event] = [])).push(fn);
      return this;
    }

    off(event, fn) {
      const list = this._events[event];
      if (!list) return this;
      if (!fn) {
        delete this._events[event];
        return this;
      }
      this._events[event] = list.filter(f => f !== fn);
      return this;
    }

    _emit(event, ...args) {
      const list = this._events[event];
      if (list) list.slice().forEach(fn => fn(...args));
    }
  }

  // ========== ClipperSDK ==========

  class ClipperSDK extends EventEmitter {
    /**
     * @param {Object} opts
     * @param {string} opts.server   — WebSocket URL, 如 'ws://localhost:8765'
     * @param {string} [opts.room]   — 4 位數字配對碼, 不指定則自動產生
     * @param {string} [opts.displayName] — 顯示名稱
     * @param {number} [opts.reconnectMaxDelay=30000] — 重連最大間隔(ms)
     * @param {number} [opts.heartbeatInterval=10000]  — 心跳間隔(ms)
     */
    constructor(opts) {
      super();
      if (!opts || !opts.server) {
        throw new Error('ClipperSDK: server URL is required');
      }

      this.server = opts.server;
      this.room = opts.room || null;
      this.displayName = opts.displayName || '';
      this.reconnectMaxDelay = opts.reconnectMaxDelay || 30000;
      this.heartbeatInterval = opts.heartbeatInterval || 10000;
      this.stunServer = opts.stunServer || 'stun:stun.l.google.com:19302';

      // 內部狀態
      this._ws = null;
      this._peerId = null;
      this._connected = false;
      this._readOnly = false;
      this._manualDisconnect = false;
      this._reconnectTimer = null;
      this._reconnectDelay = 1000;
      this._heartbeatTimer = null;
      this._pending = {};
      this._peers = new Map();
      this._fileReceives = new Map();
      this._fileSendQueue = [];        // 檔案傳送佇列
      this._fileSending = false;       // 是否正在傳送中

      // 快取資料
      this._state = {
        room: null,
        noticePosts: [],
        checklists: [],
        keyManagements: [],
        peers: [],
        chatMessages: [],
        serverTime: 0,
        ntpOffset: 0,
      };
    }

    // ==================== 連線管理 ====================

    /**
     * 建立 WebSocket 連線 → 自動 join room
     */
    connect() {
      if (this._ws && (this._ws.readyState === WebSocket.OPEN || this._ws.readyState === WebSocket.CONNECTING)) {
        return;
      }
      this._manualDisconnect = false;
      this._readOnly = false;

      try {
        this._ws = new WebSocket(this.server);
      } catch (err) {
        this._emit('error', err);
        this._scheduleReconnect();
        return;
      }

      this._ws.onopen = () => {
        this._connected = true;
        this._reconnectDelay = 1000; // reset backoff
        this._emit('connected');

        // Auto-join: 若已有 room code 直接 join, 否則 generate
        if (this.room) {
          this._send({ type: 'join', room: this.room, displayName: this.displayName });
        } else {
          this._send({ type: 'generate' });
        }
      };

      this._ws.onmessage = (evt) => {
        let data;
        try {
          data = JSON.parse(evt.data);
        } catch (_) {
          return;
        }
        this._handleMessage(data);
      };

      this._ws.onclose = () => {
        this._connected = false;
        this._stopHeartbeat();
        this._emit('disconnected');

        if (!this._manualDisconnect) {
          this._readOnly = true;
          this._emit('readonly', true);
          this._scheduleReconnect();
        }
      };

      this._ws.onerror = (err) => {
        this._emit('error', err);
      };
    }

    /**
     * 手動斷線（不觸發 readonly）
     */
    disconnect() {
      this._manualDisconnect = true;
      this._stopHeartbeat();
      this._cancelReconnect();
      this._readOnly = false;
      this._emit('readonly', false);

      if (this._ws) {
        try { this._ws.close(); } catch (_) { /* ignore */ }
        this._ws = null;
      }
      this._connected = false;
      this._peerId = null;
    }

    // ==================== 唯讀狀態 ====================

    get readOnly() {
      return this._readOnly;
    }

    get connected() {
      return this._connected;
    }

    get peerId() {
      return this._peerId;
    }

    // ==================== 公開方法 ====================

    /**
     * 發送聊天訊息
     * @param {string} text
     * @returns {boolean} 是否成功發送
     */
    sendChat(text) {
      if (!text || !this._connected || !this._state.room) return false;
      const msg = {
        from: this.displayName || this._peerId,
        text: text,
        timestamp: Date.now(),
      };
      // Optimistic local store
      this._state.chatMessages.push(msg);
      this._emit('chat', msg);

      // Try P2P via DataChannel first, fallback to WS relay
      let sentP2P = false;
      const dcPayload = {type: 'chat', from: msg.from, text: msg.text, timestamp: msg.timestamp};
      for (const [pid, ps] of this._peers) {
        if (ps && ps.dc && ps.dc.readyState === 'open') {
          try { ps.dc.send(JSON.stringify(dcPayload)); sentP2P = true; } catch (_) {}
        }
      }
      // Always send chat-backup for server persistence
      this._send({
        type: 'chat-backup',
        room: this._state.room,
        text: msg.text,
        from: msg.from,
        timestamp: msg.timestamp,
      });
      // Only relay if no P2P available
      if (!sentP2P) {
        const payload = {type: 'chat', from: msg.from, text: msg.text, timestamp: msg.timestamp, msgId: this._uuid()};
        for (const [peerId] of this._peers) {
          this._send({
            type: 'relay-data',
            room: this._state.room,
            to: peerId,
            data: payload,
          });
        }
      }
      return true;
    }

    /**
     * 建立公告
     */
    createNotice(title, content, category, tags) {
      if (!this._connected || !this._state.room) return false;
      const post = {
        id: this._uuid(),
        title: title || '',
        content: content || '',
        author: this.displayName || this._peerId,
        category: category || '',
        tags: tags || [],
        color: '#38bdf8',
        pinned: false,
        timestamp: Date.now(),
      };
      // Optimistic local update (server broadcasts to other peers only)
      this._state.noticePosts = this._state.noticePosts.filter(p => p.id !== post.id);
      this._state.noticePosts.push(post);
      this._emit('notice', post);
      this._send({ type: 'notice-create', room: this._state.room, post });
      return true;
    }

    /**
     * 編輯公告
     */
    editNotice(id, updates) {
      if (!this._connected || !this._state.room || !id) return false;
      this._send({
        type: 'notice-edit',
        room: this._state.room,
        id,
        ...updates,
        editedAt: Date.now(),
      });
      return true;
    }

    /**
     * 刪除公告
     */
    deleteNotice(id) {
      if (!this._connected || !this._state.room || !id) return false;
      this._send({ type: 'notice-delete', room: this._state.room, id });
      return true;
    }

    /**
     * 建立檢查清單 Board
     */
    createChecklist(title, category, tags, color) {
      if (!this._connected || !this._state.room) return false;
      const board = {
        id: this._uuid(),
        title: title || '',
        category: category || '',
        tags: tags || [],
        color: color || '#38bdf8',
        pinned: false,
        createdBy: this.displayName || this._peerId,
        createdAt: Date.now(),
        items: [],
      };
      // Optimistic local update
      this._state.checklists = this._state.checklists.filter(b => b.id !== board.id);
      this._state.checklists.push(board);
      this._send({ type: 'checklistboard-create', room: this._state.room, board });
      return true;
    }

    /**
     * 在 Board 中新增項目
     */
    addChecklistItem(boardId, text) {
      if (!this._connected || !this._state.room || !boardId) return false;
      const item = {
        id: this._uuid(),
        text: text || '',
        addedBy: this.displayName || this._peerId,
        checked: false,
        checkedAt: null,
        createdAt: Date.now(),
      };
      this._send({ type: 'checklist-add', room: this._state.room, checklistId: boardId, item });
      return true;
    }

    /**
     * 切換項目勾選狀態
     */
    toggleChecklistItem(boardId, itemId, checked) {
      if (!this._connected || !this._state.room || !boardId || !itemId) return false;
      this._send({
        type: 'checklist-toggle',
        room: this._state.room,
        checklistId: boardId,
        id: itemId,
        checked: !!checked,
        checkedAt: checked ? Date.now() : null,
      });
      return true;
    }

    /**
     * 刪除 Board 中的項目
     */
    deleteChecklistItem(boardId, itemId) {
      if (!this._connected || !this._state.room || !boardId || !itemId) return false;
      this._send({ type: 'checklist-delete', room: this._state.room, checklistId: boardId, id: itemId });
      return true;
    }

    /**
     * 建立密鑰項目
     */
    createKeyEntry(label, key, url, program) {
      if (!this._connected || !this._state.room) return false;
      const entry = {
        id: this._uuid(),
        label: label || '',
        streamKey: key || '',
        streamUrl: url || '',
        currentProgram: program || '',
        isActive: true,
        updatedAt: Date.now(),
      };
      // Optimistic local update
      this._state.keyManagements = this._state.keyManagements.filter(e => e.id !== entry.id);
      this._state.keyManagements.push(entry);
      this._send({ type: 'keymgmt-create', room: this._state.room, entry });
      return true;
    }

    /**
     * 編輯密鑰項目
     */
    editKeyEntry(id, updates) {
      if (!this._connected || !this._state.room || !id) return false;
      this._send({
        type: 'keymgmt-edit',
        room: this._state.room,
        id,
        ...updates,
        updatedAt: Date.now(),
      });
      return true;
    }

    /**
     * 刪除密鑰項目
     */
    deleteKeyEntry(id) {
      if (!this._connected || !this._state.room || !id) return false;
      this._send({ type: 'keymgmt-delete', room: this._state.room, id });
      return true;
    }

    /**
     * 切換密鑰啟用狀態
     */
    toggleKeyActive(id) {
      if (!this._connected || !this._state.room || !id) return false;
      this._send({ type: 'keymgmt-toggle-active', room: this._state.room, id });
      return true;
    }

    /**
     * 請求最新房間狀態 (Promise)
     * @returns {Promise<Object>}
     */
    fetchState() {
      return new Promise((resolve, reject) => {
        if (!this._connected || !this._state.room) {
          reject(new Error('Not connected'));
          return;
        }
        const reqId = this._uuid();
        const timeout = setTimeout(() => {
          delete this._pending[reqId];
          reject(new Error('fetchState timeout'));
        }, 10000);

        this._pending[reqId] = (state) => {
          clearTimeout(timeout);
          delete this._pending[reqId];
          resolve(state);
        };

        this._send({ type: 'state-get', room: this._state.room });
      });
    }

    // ==================== 資料讀取 ====================
    // ==================== 檔案傳輸 ====================

    /**
     * 觸發瀏覽器檔案選擇對話框，回傳所選 FileList
     * @returns {Promise<FileList|null>}
     */
    pickFiles() {
      return new Promise((resolve) => {
        const input = document.createElement('input');
        input.type = 'file';
        input.multiple = true;
        input.onchange = () => resolve(input.files);
        input.click();
      });
    }

    /**
     * 發送檔案到指定 peer
     * @param {string} peerId
     * @param {File} file
     */
    async sendFile(peerId, file) {
      if (!this._connected || !this._state.room || !file) {
        this._emit('file-error', { peerId, fileId: null, message: 'Not connected or invalid file' });
        return;
      }
      // 加入佇列
      const entry = { peerId, file, fileId: this._uuid(), name: file.name, size: file.size, status: 'pending' };
      this._fileSendQueue.push(entry);
      this._emit('file-progress', { peerId, fileId: entry.fileId, name: entry.name, progress: 0, status: 'queued' });
      // 如果沒有正在傳送中，啟動佇列
      if (!this._fileSending) {
        await this._sendNextFile();
      }
    }

    async _sendNextFile() {
      if (this._fileSendQueue.length === 0) {
        this._fileSending = false;
        return;
      }
      this._fileSending = true;
      const entry = this._fileSendQueue.shift();
      const { peerId, file, fileId, name } = entry;

      try {
        const chunkSize = 65536; // 64KB
        const buffer = await file.arrayBuffer();
        const totalChunks = Math.ceil(buffer.byteLength / chunkSize);

        // Compute SHA-256 checksum (like native clipper)
        let sha256 = null;
        if (crypto.subtle) {
          try {
            const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
            sha256 = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
          } catch (_) {}
        }

        // Check if P2P DataChannel is available
        const ps = this._peers.get(peerId);
        const useDc = ps && ps.dc && ps.dc.readyState === 'open';

        if (useDc) {
          // P2P: send raw ArrayBuffer over DataChannel (no base64 overhead)
          const meta = { type: 'file-meta', fileId, name, size: file.size, chunks: totalChunks };
          if (sha256) meta.sha256 = sha256;
          ps.dc.send(JSON.stringify(meta));

          for (let i = 0; i < totalChunks; i++) {
            const start = i * chunkSize;
            if (ps.dc.bufferedAmount > 65536) {
              await new Promise(r => { ps.dc.addEventListener('bufferedamountlow', r, { once: true }); });
            }
            try { ps.dc.send(buffer.slice(start, start + Math.min(chunkSize, buffer.byteLength - start))); } catch (_) {}
            this._emit('file-progress', { peerId, fileId, name, progress: Math.round((i + 1) / totalChunks * 100), status: 'sending' });
          }
          ps.dc.send(JSON.stringify({ type: 'file-done', fileId }));
        } else {
          // WS relay (base64) — existing code
          const meta = { type: 'file-meta', fileId, name, size: file.size, chunks: totalChunks };
          if (sha256) meta.sha256 = sha256;
          this._send({
            type: 'relay-data', room: this._state.room, to: peerId, data: meta,
          });

          for (let i = 0; i < totalChunks; i++) {
            const start = i * chunkSize;
            const end = Math.min(start + chunkSize, buffer.byteLength);
            const b64 = this._arrayBufferToBase64(buffer.slice(start, end));
            this._send({
              type: 'relay-chunk', room: this._state.room, to: peerId,
              fileId, chunk: b64, index: i, total: totalChunks,
            });
            await this._sleep(10);
            this._emit('file-progress', { peerId, fileId, name, progress: Math.round((i + 1) / totalChunks * 100), status: 'sending' });
          }

          this._send({
            type: 'relay-data', room: this._state.room, to: peerId,
            data: { type: 'file-done', fileId },
          });
        }

        this._emit('file-sent', { peerId, fileId, name });
      } catch (err) {
        this._emit('file-error', { peerId, fileId, message: err.message || 'Send failed' });
      }

      // Process next in queue
      await this._sendNextFile();
    }

    /**
     * 取消檔案傳送（從 queue 移除 + 通知對方）
     * @param {string} peerId
     * @param {string} fileId
     */
    cancelFile(peerId, fileId) {
      if (!this._connected || !this._state.room) return;
      // 從發送佇列移除（防止 queue 卡住）
      this._fileSendQueue = this._fileSendQueue.filter(function(e) { return e.fileId !== fileId; });
      // 通知接收方取消
      this._send({ type: 'file-cancel', room: this._state.room, to: peerId, fileId });
      // 取消本機接收
      const entry = this._fileReceives.get(fileId);
      if (entry) {
        entry.status = 'cancelled';
        this._fileReceives.delete(fileId);
      }
      this._emit('file-error', { peerId, fileId, message: 'Transfer cancelled' });
    }

    /**
     * 更新顯示名稱（同步到 server，server 廣播 peer-list 給所有人）
     * 此操作保持靜默 — 不產生 system 訊息
     * @param {string} name - 新顯示名稱
     */
    setDisplayName(name) {
      if (!name || !name.trim()) return;
      this.displayName = name.trim();
      if (this._connected && this._state.room) {
        this._send({ type: 'register-name', room: this._state.room, displayName: this.displayName });
      }
    }

    /**
     * 重新傳送失敗的檔案（重新加入 queue）
     * @param {string} peerId
     * @param {string} fileId
     * @param {File} file
     */
    retryFile(peerId, fileId, file) {
      this.sendFile(peerId, file);
    }

    /**
     * 取得發送佇列狀態（給 UI 使用）
     * @returns {Array} [{peerId, fileId, name, size, status}]
     */
    getSendQueue() {
      return this._fileSendQueue.map(function(e) { return { peerId: e.peerId, fileId: e.fileId, name: e.name, size: e.size, status: e.status }; });
    }

    /**
     * 取得接收佇列狀態
     * @returns {Array} [{fileId, name, size, progress, status, from}]
     */
    getReceiveQueue() {
      var result = [];
      for (const [fileId, entry] of this._fileReceives) {
        result.push({ fileId, name: entry.name, size: entry.size, progress: entry.progress || 0, status: entry.status, from: entry.from, fromDisplayName: entry.fromDisplayName || entry.from });
      }
      return result;
    }

    // ==================== 內部：檔案傳輸工具 ====================

    _arrayBufferToBase64(buffer) {
      const bytes = new Uint8Array(buffer);
      let binary = '';
      for (let i = 0; i < bytes.byteLength; i++) {
        binary += String.fromCharCode(bytes[i]);
      }
      return btoa(binary);
    }

    _sleep(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }


    getNotices() {
      return this._state.noticePosts.slice();
    }

    getChecklists() {
      return this._state.checklists.slice();
    }

    getKeyEntries() {
      return this._state.keyManagements.slice();
    }

    getChatMessages() {
      return this._state.chatMessages.slice();
    }

    // ==================== 內部：訊息處理 ====================

    _handleMessage(data) {
      const type = data.type;

      switch (type) {

        // ---- 房間與連線 ----
        case 'generated':
          this.room = data.room;
          this._state.room = data.room;
          this._send({ type: 'join', room: data.room, displayName: this.displayName });
          break;

        case 'joined':
          this._peerId = data.peerId;
          this._state.room = data.room;
          if (!this.room) this.room = data.room;
          // 解除唯讀
          if (this._readOnly) {
            this._readOnly = false;
            this._emit('readonly', false);
          }
          // 發送註冊、狀態、時間請求
          this._send({ type: 'register-name', room: data.room, displayName: this.displayName });
          this._send({ type: 'state-get', room: data.room });
          this._send({ type: 'time-request' });
          this._startHeartbeat();
          break;

        case 'room_full':
          this._emit('error', new Error('Room is full'));
          break;

        case 'error':
          this._emit('error', new Error(data.message || 'Unknown error'));
          break;

        // ---- 名稱 ----
        case 'name-resolved':
          this.displayName = data.displayName;
          this._emit('name-updated', { displayName: data.displayName });
          break;

        // ---- 房間狀態 ----
        case 'room-state':
          this._mergeState(data);
          this._emit('state', this._getPublicState());
          // resolve pending fetchState
          for (const id of Object.keys(this._pending)) {
            this._pending[id](this._getPublicState());
          }
          break;

        // ---- 時間同步 ----
        case 'time-sync':
          this._state.serverTime = data.serverTime;
          this._state.ntpOffset = data.serverTime - Date.now();
          break;

        // ---- 心跳 ----
        case 'pong':
          // 心跳確認，無需特殊處理
          break;

        // ---- 成員管理 ----
        case 'room_peers':
          if (data.peers) {
            const existingPeers = this._peers;
            this._state.peers = data.peers;
            this._peers = new Map(data.peers.map(p => {
              const existing = existingPeers.get(p.peerId);
              return [p.peerId, existing ? { ...p, pc: existing.pc, dc: existing.dc, connected: existing.connected, relay: existing.relay } : { ...p, pc: null, dc: null, connected: false, relay: false }];
            }));
            data.peers.forEach(p => {
              this._emit('peer-joined', p);
              this._connectToPeer(p.peerId);
            });
          }
          break;

        case 'peer-list':
          if (data.peers) {
            const oldIds = new Set(this._state.peers.map(p => p.peerId));
            const newIds = new Set(data.peers.map(p => p.peerId));
            // 建立舊名稱對照表（偵測名稱變更）
            const oldNames = new Map();
            this._state.peers.forEach(p => oldNames.set(p.peerId, p.displayName));
            // 離開的 peer
            this._state.peers.forEach(p => {
              if (!newIds.has(p.peerId)) {
                const ps = this._peers.get(p.peerId);
                if (ps) {
                  if (ps.dc) try { ps.dc.close(); } catch (_) {}
                  if (ps.pc) try { ps.pc.close(); } catch (_) {}
                }
                this._peers.delete(p.peerId);
                const leftInfo1 = { peerId: p.peerId, displayName: p.displayName || '' };
                this._emit('peer-left', leftInfo1);
              }
            });
            this._state.peers = data.peers;
            const existingPeers = this._peers;
            this._peers = new Map(data.peers.map(p => {
              const existing = existingPeers.get(p.peerId);
              return [p.peerId, existing ? { ...p, pc: existing.pc, dc: existing.dc, connected: existing.connected, relay: existing.relay } : { ...p, pc: null, dc: null, connected: false, relay: false }];
            }));
            // 新加入或名稱變更的 peer
            data.peers.forEach(p => {
              if (!oldIds.has(p.peerId)) {
                this._emit('peer-joined', p);
                this._connectToPeer(p.peerId);
              } else if (oldNames.get(p.peerId) !== p.displayName) {
                // 名稱已變更 → 發射 peer-updated 事件
                this._emit('peer-updated', p);
              }
            });
          }
          break;

        case 'peer_joined':
          {
            // 保留已有的 displayName（若有），避免 server 未送時覆蓋
            const existing = this._peers.get(data.peerId);
            const displayName = data.displayName || (existing ? existing.displayName : '') || '新成員';
            const peer = { peerId: data.peerId, displayName: displayName, pc: null, dc: null, connected: false, relay: false };
            if (existing) { peer.pc = existing.pc; peer.dc = existing.dc; peer.connected = existing.connected; peer.relay = existing.relay; }
            this._peers.set(data.peerId, peer);
            if (!this._state.peers.find(p => p.peerId === data.peerId)) {
              this._state.peers.push(peer);
            }
            this._emit('peer-joined', peer);
            this._connectToPeer(data.peerId);
          }
          break;

        case 'peer_left':
          {
            const ps = this._peers.get(data.peerId);
            if (ps) {
              if (ps.dc) try { ps.dc.close(); } catch (_) {}
              if (ps.pc) try { ps.pc.close(); } catch (_) {}
            }
            const leftPeer = { peerId: data.peerId, displayName: data.displayName || (ps ? ps.displayName : '') || '' };
            this._peers.delete(data.peerId);
            this._state.peers = this._state.peers.filter(p => p.peerId !== data.peerId);
            this._emit('peer-left', leftPeer);
          }
          break;

        // ---- P2P WebRTC Signaling ----
        case 'offer':
          this._startWebRTCPeer(data.from, false, data.data);
          break;

        case 'answer':
          {
            const peerState = this._peers.get(data.from);
            if (peerState && peerState.pc && peerState.pc.remoteDescription === null) {
              peerState.pc.setRemoteDescription(new RTCSessionDescription(data.data));
            }
          }
          break;

        case 'ice-candidate':
          {
            const peerState2 = this._peers.get(data.from);
            if (peerState2 && peerState2.pc) {
              peerState2.pc.addIceCandidate(new RTCIceCandidate(data.data));
            }
          }
          break;

        // ---- 公告廣播 ----
        case 'notice-create':
          if (data.post) {
            this._state.noticePosts = this._state.noticePosts.filter(p => p.id !== data.post.id);
            this._state.noticePosts.push(data.post);
            this._emit('notice', data.post);
          }
          break;

        case 'notice-edit':
          {
            const post = this._state.noticePosts.find(p => p.id === data.id);
            if (post) {
              if (data.title !== undefined) post.title = data.title;
              if (data.content !== undefined) post.content = data.content;
              if (data.category !== undefined) post.category = data.category;
              if (data.tags !== undefined) post.tags = data.tags;
              if (data.color !== undefined) post.color = data.color;
              post.editedAt = data.editedAt;
              this._emit('notice', post);
            }
          }
          break;

        case 'notice-delete':
          this._state.noticePosts = this._state.noticePosts.filter(p => p.id !== data.id);
          break;

        case 'notice-pin':
          {
            const post = this._state.noticePosts.find(p => p.id === data.id);
            if (post) {
              post.pinned = data.pinned;
              this._emit('notice', post);
            }
          }
          break;

        // ---- 檢查清單廣播 ----
        case 'checklistboard-create':
          if (data.board) {
            this._state.checklists = this._state.checklists.filter(b => b.id !== data.board.id);
            this._state.checklists.push(data.board);
          }
          break;

        case 'checklistboard-edit':
          {
            const board = this._state.checklists.find(b => b.id === data.id);
            if (board) {
              if (data.title !== undefined) board.title = data.title;
              if (data.category !== undefined) board.category = data.category;
              if (data.tags !== undefined) board.tags = data.tags;
              if (data.color !== undefined) board.color = data.color;
            }
          }
          break;

        case 'checklistboard-delete':
          this._state.checklists = this._state.checklists.filter(b => b.id !== data.id);
          break;

        case 'checklistboard-pin':
          {
            const board = this._state.checklists.find(b => b.id === data.id);
            if (board) board.pinned = data.pinned;
          }
          break;

        case 'checklistboard-remind':
          {
            const board = this._state.checklists.find(b => b.id === data.id);
            if (board) {
              board.reminderAt = data.reminderAt;
              board.reminderTitle = data.reminderTitle;
            }
          }
          break;

        case 'checklist-add':
          if (data.checklistId && data.item) {
            const board = this._state.checklists.find(b => b.id === data.checklistId);
            if (board) {
              board.items = board.items.filter(i => i.id !== data.item.id);
              board.items.push(data.item);
            }
          }
          break;

        case 'checklist-toggle':
          if (data.checklistId && data.id) {
            const board = this._state.checklists.find(b => b.id === data.checklistId);
            if (board) {
              const item = board.items.find(i => i.id === data.id);
              if (item) {
                item.checked = data.checked;
                item.checkedAt = data.checkedAt;
              }
            }
          }
          break;

        case 'checklist-delete':
          if (data.checklistId && data.id) {
            const board = this._state.checklists.find(b => b.id === data.checklistId);
            if (board) {
              board.items = board.items.filter(i => i.id !== data.id);
            }
          }
          break;

        case 'checklist-reset':
          {
            const board = this._state.checklists.find(b => b.id === data.id || b.id === data.checklistId);
            if (board && board.items) {
              board.items.forEach(i => { i.checked = false; i.checkedAt = null; });
            }
          }
          break;

        // ---- 密鑰廣播 ----
        case 'keymgmt-create':
          if (data.entry) {
            this._state.keyManagements = this._state.keyManagements.filter(e => e.id !== data.entry.id);
            this._state.keyManagements.push(data.entry);
          }
          break;

        case 'keymgmt-edit':
          {
            const entry = this._state.keyManagements.find(e => e.id === data.id);
            if (entry) {
              if (data.label !== undefined) entry.label = data.label;
              if (data.streamKey !== undefined) entry.streamKey = data.streamKey;
              if (data.streamUrl !== undefined) entry.streamUrl = data.streamUrl;
              if (data.currentProgram !== undefined) entry.currentProgram = data.currentProgram;
            }
          }
          break;

        case 'keymgmt-delete':
          this._state.keyManagements = this._state.keyManagements.filter(e => e.id !== data.id);
          break;

        case 'keymgmt-toggle-active':
          {
            const entry = this._state.keyManagements.find(e => e.id === data.id);
            if (entry) entry.isActive = data.isActive;
          }
          break;

        case 'keymgmt-set-program':
          {
            const entry = this._state.keyManagements.find(e => e.id === data.id);
            if (entry) entry.currentProgram = data.currentProgram;
          }
          break;

        // ---- WS 中繼（聊天、檔案等） ----
        case 'relay-data':
          if (data.data && data.data.type === 'chat') {
            const chatMsg = {
              from: data.data.from || data.from,
              text: data.data.text,
              timestamp: data.data.timestamp,
            };
            this._state.chatMessages.push(chatMsg);
            this._emit('chat', chatMsg);
          } else if (data.data && data.data.type === 'file-meta') {
            const meta = data.data;
            const fromPeer = this._peers.get(data.from);
            const fromDisplayName = fromPeer ? (fromPeer.displayName || data.from) : data.from;
            this._fileReceives.set(meta.fileId, {
              fileId: meta.fileId,
              name: meta.name,
              size: meta.size,
              chunks: meta.chunks || 0,
              sha256: meta.sha256 || null,
              received: 0,
              blobs: [],
              chunkCount: 0,
              from: data.from,
              fromDisplayName: fromDisplayName,
              status: 'receiving',
            });
            this._emit('file-meta', { fileId: meta.fileId, name: meta.name, size: meta.size, chunks: meta.chunks, from: data.from, fromDisplayName: fromDisplayName });
          } else if (data.data && data.data.type === 'file-done') {
            const entry = this._fileReceives.get(data.data.fileId);
            if (entry && entry.status !== 'cancelled') {
              entry.status = 'done';
              const blob = new Blob(entry.blobs);
              this._fileReceives.delete(data.data.fileId);

              // SHA-256 verification (like native clipper)
              var verified = null;
              if (entry.sha256 && crypto.subtle) {
                (async () => {
                  try {
                    const buf = await blob.arrayBuffer();
                    const hashBuffer = await crypto.subtle.digest('SHA-256', buf);
                    const hash = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
                    verified = (hash === entry.sha256);
                  } catch (_) {}
                })();
              }

              this._emit('file-done', { fileId: data.data.fileId, name: entry.name, blob, size: entry.size, from: entry.from, sha256: entry.sha256, verified: verified });
            }
          }
          break;
        // ---- 檔案傳輸：中繼 chunk ----
        case 'relay-chunk':
          {
            const entry = this._fileReceives.get(data.fileId);
            if (entry && entry.status !== 'cancelled') {
              const binaryStr = atob(data.chunk);
              const bytes = new Uint8Array(binaryStr.length);
              for (let i = 0; i < binaryStr.length; i++) bytes[i] = binaryStr.charCodeAt(i);
              entry.blobs.push(bytes.buffer);
              entry.received += bytes.length;
              entry.chunkCount = (entry.chunkCount || 0) + 1;
              const progress = entry.chunks > 0 ? Math.round(entry.chunkCount / entry.chunks * 100) : Math.round(entry.received / entry.size * 100);
              entry.progress = progress;
              if (progress % 10 === 0 || progress === 100) {
                this._emit('file-chunk', { fileId: data.fileId, index: data.index, total: data.total, progress, from: data.from });
              }
            }
          }
          break;

        // ---- 檔案傳輸：取消 ----
        case 'file-cancel':
          {
            const entry = this._fileReceives.get(data.fileId);
            if (entry) {
              entry.status = 'cancelled';
              this._fileReceives.delete(data.fileId);
              this._emit('file-error', { peerId: data.from, fileId: data.fileId, message: 'Transfer cancelled' });
            }
          }
          break;

        // ---- 未知類型 ----
        default:
          // 忽略已知但未處理的類型，未知類型也靜默
          break;
      }
    }

    // ==================== 內部：狀態合併 ====================

    _mergeState(data) {
      // 過濾已刪除 ID（防幽靈復活）
      if (data.deletedNoticeIds) {
        const delSet = new Set(data.deletedNoticeIds);
        this._state.noticePosts = this._state.noticePosts.filter(p => !delSet.has(p.id));
      }
      if (data.deletedChecklistIds) {
        const delSet = new Set(data.deletedChecklistIds);
        this._state.checklists = this._state.checklists.filter(b => !delSet.has(b.id));
      }
      if (data.deletedKeyIds) {
        const delSet = new Set(data.deletedKeyIds);
        this._state.keyManagements = this._state.keyManagements.filter(e => !delSet.has(e.id));
      }

      // 合併（伺服器優先 + 保留本地特有）
      if (data.noticePosts) {
        const serverIds = new Set(data.noticePosts.map(p => p.id));
        const localOnly = this._state.noticePosts.filter(p => !serverIds.has(p.id));
        this._state.noticePosts = [...data.noticePosts, ...localOnly];
      }
      if (data.checklists) {
        const serverIds = new Set(data.checklists.map(b => b.id));
        const localOnly = this._state.checklists.filter(b => !serverIds.has(b.id));
        this._state.checklists = [...data.checklists, ...localOnly];
        // 深度合併 items
        for (const serverBoard of data.checklists) {
          const localBoard = this._state.checklists.find(b => b.id === serverBoard.id);
          if (localBoard && serverBoard.items) {
            const serverItemIds = new Set(serverBoard.items.map(i => i.id));
            const localOnlyItems = localBoard.items.filter(i => !serverItemIds.has(i.id));
            localBoard.items = [...serverBoard.items, ...localOnlyItems];
          }
        }
      }
      if (data.keyManagements) {
        const serverIds = new Set(data.keyManagements.map(e => e.id));
        const localOnly = this._state.keyManagements.filter(e => !serverIds.has(e.id));
        this._state.keyManagements = [...data.keyManagements, ...localOnly];
      }
    }

    _getPublicState() {
      return {
        room: this._state.room,
        noticePosts: this._state.noticePosts.slice(),
        checklists: this._state.checklists.slice(),
        keyManagements: this._state.keyManagements.slice(),
        peers: this._state.peers.slice(),
      };
    }

    // ==================== 內部：心跳 ====================

    _startHeartbeat() {
      this._stopHeartbeat();
      this._heartbeatTimer = setInterval(() => {
        if (this._ws && this._ws.readyState === WebSocket.OPEN && this._state.room) {
          this._send({ type: 'ping' });
        }
      }, this.heartbeatInterval);
    }

    _stopHeartbeat() {
      if (this._heartbeatTimer) {
        clearInterval(this._heartbeatTimer);
        this._heartbeatTimer = null;
      }
    }

    // ==================== 內部：重連 ====================

    _scheduleReconnect() {
      this._cancelReconnect();
      this._reconnectTimer = setTimeout(() => {
        this._reconnectDelay = Math.min(this._reconnectDelay * 2, this.reconnectMaxDelay);
        this.connect();
      }, this._reconnectDelay);
    }

    _cancelReconnect() {
      if (this._reconnectTimer) {
        clearTimeout(this._reconnectTimer);
        this._reconnectTimer = null;
      }
    }

    // ==================== 內部：傳送 ====================

    _send(obj) {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        this._ws.send(JSON.stringify(obj));
        return true;
      }
      return false;
    }

    // ==================== 內部：工具 ====================

    _uuid() {
      // 簡易 UUID v4
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
      });
    }

    // ==================== WebRTC P2P ====================

    /**
     * Initiate P2P connection to a peer.
     * Lower peerId creates the offer (initiator), higher waits (responder).
     */
    _connectToPeer(targetPeerId) {
      if (this._peers.has(targetPeerId) && this._peers.get(targetPeerId).pc) return;
      if (targetPeerId === this._peerId) return;
      // Lower peerId creates the offer (initiator), higher waits (responder)
      if (this._peerId < targetPeerId) {
        this._startWebRTCPeer(targetPeerId, true, null);
      } else {
        // Create a placeholder so we know about this peer
        if (!this._peers.has(targetPeerId)) {
          this._peers.set(targetPeerId, { pc: null, dc: null, connected: false, relay: false });
        }
      }
    }

    /**
     * Start WebRTC RTCPeerConnection with a peer.
     * @param {string} targetPeerId
     * @param {boolean} isInitiator
     * @param {Object|null} remoteOffer — SDP offer from the remote peer
     */
    _startWebRTCPeer(targetPeerId, isInitiator, remoteOffer) {
      const existing = this._peers.get(targetPeerId);
      if (existing && existing.pc) {
        if (!remoteOffer) return; // already have a PC for this peer
        // If we get an offer but already have a PC, close and recreate
        try { if (existing.pc) existing.pc.close(); } catch (_) {}
        this._peers.delete(targetPeerId);
      }

      const pc = new RTCPeerConnection({ iceServers: [{ urls: this.stunServer }] });

      if (isInitiator) {
        const dc = pc.createDataChannel('clipper', { ordered: true });
        this._setupDataChannel(dc, targetPeerId);
      }

      pc.ondatachannel = (event) => {
        this._setupDataChannel(event.channel, targetPeerId);
      };

      pc.onicecandidate = (event) => {
        if (event.candidate && this._state.room) {
          this._send({
            type: 'ice-candidate', room: this._state.room,
            to: targetPeerId,
            data: { candidate: event.candidate.candidate, sdpMid: event.candidate.sdpMid, sdpMLineIndex: event.candidate.sdpMLineIndex }
          });
        }
      };

      pc.oniceconnectionstatechange = () => {
        if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
          const ps = this._peers.get(targetPeerId);
          if (ps) ps.connected = true;
          this._emit('transport', { peerId: targetPeerId, mode: 'p2p' });
        } else if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected') {
          const ps = this._peers.get(targetPeerId);
          if (ps) { ps.relay = true; ps.connected = true; }
          this._emit('transport', { peerId: targetPeerId, mode: 'relay' });
        }
      };

      // Store
      this._peers.set(targetPeerId, { pc, dc: null, connected: false, relay: false });

      // Initiator: create offer
      if (isInitiator) {
        pc.createOffer()
          .then(offer => pc.setLocalDescription(offer))
          .then(() => {
            if (this._state.room && pc.localDescription) {
              this._send({
                type: 'offer', room: this._state.room, to: targetPeerId,
                data: { type: pc.localDescription.type, sdp: pc.localDescription.sdp }
              });
            }
          })
          .catch(e => console.warn('[SDK:P2P] createOffer error', e));
      } else if (remoteOffer) {
        pc.setRemoteDescription(new RTCSessionDescription(remoteOffer))
          .then(() => pc.createAnswer())
          .then(answer => pc.setLocalDescription(answer))
          .then(() => {
            if (this._state.room && pc.localDescription) {
              this._send({
                type: 'answer', room: this._state.room, to: targetPeerId,
                data: { type: pc.localDescription.type, sdp: pc.localDescription.sdp }
              });
            }
          })
          .catch(e => console.warn('[SDK:P2P] createAnswer error', e));
      }
    }

    /**
     * Set up a DataChannel for a peer.
     */
    _setupDataChannel(dc, peerId) {
      dc.binaryType = 'arraybuffer';

      dc.onopen = () => {
        const ps = this._peers.get(peerId);
        if (ps) { ps.dc = dc; ps.connected = true; }
        this._emit('transport', { peerId, mode: 'p2p' });
      };

      dc.onclose = () => {
        // Fallback to relay
        const ps = this._peers.get(peerId);
        if (ps) { ps.dc = null; ps.relay = true; ps.connected = true; }
        this._emit('transport', { peerId, mode: 'relay' });
      };

      // Handle incoming DC messages (chat, file-meta, file-done)
      dc.onmessage = (event) => {
        if (typeof event.data === 'string') {
          try {
            const msg = JSON.parse(event.data);
            if (msg.type === 'chat') {
              this._state.chatMessages.push({ from: msg.from, text: msg.text, timestamp: msg.timestamp });
              this._emit('chat', { from: msg.from, text: msg.text, timestamp: msg.timestamp });
            } else if (msg.type === 'file-meta') {
              const dcFromPeer = this._peers.get(peerId);
              const dcFromName = dcFromPeer ? (dcFromPeer.displayName || peerId) : peerId;
              this._fileReceives.set(msg.fileId, {
                fileId: msg.fileId, name: msg.name, size: msg.size, chunks: msg.chunks || 0,
                sha256: msg.sha256 || null, received: 0, blobs: [], chunkCount: 0,
                from: peerId, fromDisplayName: dcFromName, status: 'receiving'
              });
              this._emit('file-meta', { fileId: msg.fileId, name: msg.name, size: msg.size, chunks: msg.chunks, from: peerId, fromDisplayName: dcFromName });
            } else if (msg.type === 'file-done') {
              const entry = this._fileReceives.get(msg.fileId);
              if (entry && entry.status !== 'cancelled') {
                entry.status = 'done';
                const blob = new Blob(entry.blobs);
                this._fileReceives.delete(msg.fileId);
                this._emit('file-done', { fileId: msg.fileId, name: entry.name, blob, size: entry.size, from: entry.from });
              }
            }
          } catch (_) {}
        } else if (event.data instanceof ArrayBuffer) {
          // File chunk received via P2P — route to file receive handler
          // Find the fileId for this peer
          for (const [fileId, entry] of this._fileReceives) {
            if (entry.from === peerId && entry.status === 'receiving') {
              entry.blobs.push(event.data);
              entry.received += event.data.byteLength;
              entry.chunkCount = (entry.chunkCount || 0) + 1;
              const progress = entry.chunks > 0 ? Math.round(entry.chunkCount / entry.chunks * 100) : Math.round(entry.received / entry.size * 100);
              entry.progress = progress;
              if (progress % 10 === 0 || progress === 100) {
                this._emit('file-chunk', { fileId, index: entry.chunkCount - 1, total: entry.chunks, progress, from: peerId });
              }
              break;
            }
          }
        }
      };
    }
  }

  // ========== 匯出 ==========

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = ClipperSDK;
  } else {
    global.ClipperSDK = ClipperSDK;
  }

})(typeof window !== 'undefined' ? window : globalThis);
