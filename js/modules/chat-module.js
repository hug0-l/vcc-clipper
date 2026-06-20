/**
 * ChatModule — Clipper chat feature module.
 *
 * Extracted from clipper.html inline JS. Handles chat message rendering,
 * sending, editing, deleting, history loading, typing indicators, and
 * message acknowledgement ticks. Communicates via WSManager for relay
 * data and via MessageBus for peer lifecycle events.
 *
 * Depends on: ClipperModule (js/core/module-base.js), global APP, global
 * loadFromStorage/saveToStorage, broadcastToPeers, showPopup, escapeHtml.
 */
class ChatModule extends ClipperModule {
    constructor(bus, wsManager, options) {
        super('chat', bus, wsManager);
        const defaults = {
            messagesId: 'chatMessages',
            inputId: 'chatInput',
            searchId: 'chatSearch',
            sendBtnId: 'btnChatSend',
            clearBtnId: 'btnClearChat',
            replyBarId: 'replyBar',
            replyTextId: 'replyText',
            replyCloseId: 'replyClose',
            typingIndicatorId: 'typingIndicator',
        };
        this._opts = Object.assign({}, defaults, options || {});
        this._pageSize = 30;
        this._typingTimer = null;
        this._typingStopTimer = null;
        this._sendHandler = null;
        this._inputKeyHandler = null;
        this._clearHandler = null;
        this._searchInputHandler = null;
        this._searchKeyHandler = null;
        this._replyCloseHandler = null;
        this._typingInputHandler = null;
        this._scrollHandler = null;

        // Register WS relay-data handler (isolated)
        wsManager.onMessage('relay-data', (data) => this._handleRelayData(data), 'chat');
    }

    // ── Public API ──

    appendSystemMessage(text) {
        const timestamp = Date.now();
        APP.state.persistedChatMessages.push({ from: '系統', text, timestamp });
        saveToStorage('vcc_chat_messages', APP.state.persistedChatMessages.slice(-200));
        if (APP.state.persistedChatMessages.length > 200) {
            APP.state.persistedChatMessages = APP.state.persistedChatMessages.slice(-200);
        }
        if (APP.state._chatVisibleCount < APP.state.persistedChatMessages.length) {
            APP.state._chatVisibleCount++;
        }
        this._renderChatRange();
        requestAnimationFrame(() => {
            const cm = this._id(this._opts.messagesId);
            if (cm && APP.state._chatLockedToBottom) cm.scrollTop = cm.scrollHeight;
        });
    }

    // ── Module lifecycle ──

    _mount() {
        const saved = loadFromStorage('vcc_chat_messages', []);
        APP.state.persistedChatMessages = saved;
        APP.state._chatVisibleCount = Math.min(15, saved.length);

        this._wireEvents();
        this._renderChatRange();
        requestAnimationFrame(() => {
            const cm = this._id(this._opts.messagesId);
            if (cm) cm.scrollTop = cm.scrollHeight;
        });

        // Scroll-to-bottom tracking
        this._scrollHandler = () => {
            const el = this._id(this._opts.messagesId);
            if (!el) return;
            const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 60;
            APP.state._chatLockedToBottom = atBottom;
        };
        const el = this._id(this._opts.messagesId);
        if (el) el.addEventListener('scroll', this._scrollHandler);
    }

    _unmount() {
        this._cleanupTyping();
        this._removeEvents();
        const el = this._id(this._opts.messagesId);
        if (el && this._scrollHandler) el.removeEventListener('scroll', this._scrollHandler);
        this._scrollHandler = null;
    }

    // ── WS relay-data handler ──

    _handleRelayData(data) {
        if (!data.data) return;
        const d = data.data;
        const from = data.from;

        if (d.type === 'chat') {
            this._appendChatMessage(d.from, d.text, d.timestamp, d.msgId, d.replyTo);
            if (d.msgId && APP.state.ws && APP.state.ws.readyState === WebSocket.OPEN && APP.state.room) {
                sendWsMessage({ type: 'relay-data', room: APP.state.room, to: from, data: { type: 'ack', msgId: d.msgId } });
            }
        } else if (d.type === 'ack') {
            if (d.msgId) {
                const entry = APP.state.sentMessages.get(d.msgId);
                if (entry) {
                    entry.acks.add(from);
                    this._updateMsgTick(entry, d.msgId);
                }
            }
        } else if (d.type === 'typing') {
            this._showTyping(d.from || '某人');
        } else if (d.type === 'typing-stop') {
            this._hideTyping();
        } else if (d.type === 'chat-edit') {
            const el = document.querySelector('[data-msgid="' + d.msgId + '"]');
            if (el) {
                const textDiv = el.querySelector('.msg-text');
                if (textDiv) {
                    textDiv.textContent = d.newText;
                    let editedLabel = el.querySelector('.msg-edited');
                    if (!editedLabel) {
                        editedLabel = document.createElement('span');
                        editedLabel.className = 'msg-edited';
                        el.querySelector('.msg-time')?.appendChild(editedLabel);
                    }
                    editedLabel.textContent = ' (已編輯)';
                }
            }
        } else if (d.type === 'chat-delete') {
            const el = document.querySelector('[data-msgid="' + d.msgId + '"]');
            if (el) {
                el.classList.add('deleted');
                el.innerHTML = '<div class="msg-text" style="text-align:center;font-style:italic;color:#64748b;">此訊息已刪除</div>';
                el.querySelectorAll('.chat-msg-actions').forEach(a => a.remove());
            }
        }
    }

    // ── Send ──

    _sendChatMessage() {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        const input = this._id(this._opts.inputId);
        const text = input.value.trim();
        if (!text) { APP.showStatusMsg('💡 請輸入訊息內容'); return; }
        if (!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }

        // Clear typing state
        this._cleanupTyping();
        broadcastToPeers(JSON.stringify({ type: 'typing-stop' }));

        input.value = '';
        const timestamp = Date.now();
        const msgId = crypto.randomUUID().slice(0, 8);
        const peerCount = APP.state.peers.size || 1;
        const replyTo = APP.state._pendingReply || null;
        const msg = { type: 'chat', msgId, text, from: APP.state.displayName, timestamp };
        if (replyTo) {
            msg.replyTo = { msgId: replyTo.msgId, text: replyTo.text, from: replyTo.from };
        }

        APP.state._pendingReply = null;
        const replyBar = this._id(this._opts.replyBarId);
        if (replyBar) replyBar.style.display = 'none';

        APP.state.sentMessages.set(msgId, {
            text, timestamp, acks: new Set(), totalPeers: peerCount, div: null
        });

        // Save to local persisted messages BEFORE rendering (_renderChatRange reads from it)
        APP.state.persistedChatMessages.push({ from: APP.state.displayName, text, timestamp, msgId, replyTo });
        saveToStorage('vcc_chat_messages', APP.state.persistedChatMessages.slice(-200));
        if (APP.state.persistedChatMessages.length > 200) {
            APP.state.persistedChatMessages = APP.state.persistedChatMessages.slice(-200);
        }
        if (APP.state._chatVisibleCount < APP.state.persistedChatMessages.length) {
            APP.state._chatVisibleCount++;
        }

        broadcastToPeers(JSON.stringify(msg));
        this._renderChatMessage(APP.state.displayName, text, timestamp, msgId, replyTo);

        if (APP.state.room) {
            const backupMsg = { type: 'chat-backup', room: APP.state.room, text, from: APP.state.displayName, timestamp };
            if (replyTo) backupMsg.replyTo = replyTo;
            sendWsMessage(backupMsg);
        }
    }

    // ── Append / Persist ──

    _appendChatMessage(from, text, timestamp, msgId, replyTo) {
        const displayFrom = APP.state.peerNames.get(from) || from;
        APP.state.persistedChatMessages.push({ from, text, timestamp, msgId, replyTo });
        saveToStorage('vcc_chat_messages', APP.state.persistedChatMessages.slice(-200));
        if (APP.state.persistedChatMessages.length > 200) {
            APP.state.persistedChatMessages = APP.state.persistedChatMessages.slice(-200);
        }
        if (APP.state._chatVisibleCount < APP.state.persistedChatMessages.length) {
            APP.state._chatVisibleCount++;
        }
        this._renderChatMessage(from, text, timestamp, msgId, replyTo);
        if (from !== APP.state.displayName) {
            const short = text.length > 60 ? text.slice(0, 60) + '…' : text;
            showPopup('💬', displayFrom, short);
        }
    }

    // ── Tick indicator ──

    _updateMsgTick(entry, msgId) {
        if (!entry) return;
        const el = document.querySelector('[data-msgid="' + msgId + '"] .msg-tick');
        if (!el) return;
        const acked = entry.acks.size;
        const total = entry.totalPeers;
        if (acked >= total) {
            el.textContent = '✓✓';
            el.className = 'msg-tick delivered';
        } else {
            el.textContent = '✓';
            el.className = 'msg-tick sent';
        }
    }

    // ── Name → hue ──

    _nameToHue(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            hash = str.charCodeAt(i) + ((hash << 5) - hash);
        }
        return ((hash % 360) + 360) % 360;
    }

    // ── User color from name ──

    _userColor(name) {
        let hash = 0;
        for (let i = 0; i < name.length; i++) {
            hash = name.charCodeAt(i) + ((hash << 5) - hash);
        }
        const hue = ((hash % 360) + 360) % 360;
        return { hue, light: `hsl(${hue}, 50%, 55%)`, dark: `hsl(${hue}, 40%, 30%)` };
    }

    // ── Rendering ──

    _renderSingleChatMessage(from, text, timestamp, msgId, replyTo, container) {
        if (from === '系統') {
            const div = document.createElement('div');
            div.style.cssText = 'align-self:center;text-align:center;font-size:11px;color:#475569;padding:2px 0;opacity:0.7;';
            div.textContent = text;
            container.appendChild(div);
            return;
        }
        const displayFrom = APP.state.peerNames.get(from) || from;
        const isSelf = from === APP.state.displayName;
        const side = isSelf ? 'self' : 'other';
        const timeStr = new Date(timestamp).toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        const div = document.createElement('div');
        div.className = 'chat-msg ' + side;
        if (msgId) div.dataset.msgid = msgId;

        let innerHtml = '';

        if (replyTo) {
            const replyFrom = APP.state.peerNames.get(replyTo.from) || replyTo.from;
            innerHtml += '<div class="msg-quote"><div class="quote-from">↩ ' + escapeHtml(replyFrom) + '</div><div class="quote-text">' + escapeHtml(replyTo.text) + '</div></div>';
        }

        if (!isSelf) {
            const { hue, light, dark } = this._userColor(from);
            div.style.setProperty('--msg-bg-dark', dark);
            innerHtml += '<div class="msg-sender"><span class="msg-avatar" style="background:' + light + '">' + escapeHtml(displayFrom.charAt(0)) + '</span><span style="color:' + light + '">' + escapeHtml(displayFrom) + '</span></div>'
                + '<div class="msg-text">' + escapeHtml(text) + '</div>'
                + '<div class="msg-time">' + timeStr + '</div>';
        } else {
            const { hue, light } = this._userColor(from);
            const entry = msgId ? APP.state.sentMessages.get(msgId) : null;
            const tick = entry ? '<span class="msg-tick' + (entry.acks.size >= entry.totalPeers ? ' delivered' : ' sent') + '">' + (entry.acks.size >= entry.totalPeers ? '✓✓' : '✓') + '</span>' : '';
            innerHtml += '<div class="msg-sender"><span class="msg-avatar" style="background:' + light + '">' + escapeHtml(displayFrom.charAt(0)) + '</span>' + escapeHtml(displayFrom) + '</div>'
                + '<div class="msg-text">' + escapeHtml(text) + '</div>'
                + '<div class="msg-time">' + timeStr + ' ' + tick + '</div>';
        }

        innerHtml += '<div class="chat-msg-actions">'
            + '<button class="chat-msg-btn" data-action="reply" title="回覆">↩</button>'
            + '<button class="chat-msg-btn" data-action="menu" title="更多">⋯</button>'
            + '<div class="chat-msg-menu" data-menu-for="' + (msgId || '') + '">'
            + '<div class="chat-msg-menu-item" data-action="edit-msg">✏️ 編輯</div>'
            + '<div class="chat-msg-menu-item danger" data-action="delete-msg">🗑 刪除</div>'
            + '</div></div>';

        div.innerHTML = innerHtml;
        container.appendChild(div);

        // Wire up action buttons
        const actionsDiv = div.querySelector('.chat-msg-actions');
        if (actionsDiv && msgId) {
            actionsDiv.querySelector('[data-action="reply"]')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const senderName = APP.state.peerNames.get(from) || displayFrom;
                APP.state._pendingReply = { msgId, text, from };
                const replyBar = this._id(this._opts.replyBarId);
                const replyText = this._id(this._opts.replyTextId);
                if (replyBar && replyText) {
                    const shortText = text.length > 50 ? text.slice(0, 50) + '…' : text;
                    replyText.textContent = '↩ 回覆 @' + senderName + '：' + shortText;
                    replyBar.style.display = 'flex';
                }
                this._id(this._opts.inputId)?.focus();
            });

            actionsDiv.querySelector('[data-action="menu"]')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const menu = actionsDiv.querySelector('.chat-msg-menu');
                if (menu) menu.classList.toggle('show');
            });

            actionsDiv.querySelector('[data-action="edit-msg"]')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const menu = actionsDiv.querySelector('.chat-msg-menu');
                if (menu) menu.classList.remove('show');
                this._editChatMessage(msgId);
            });

            actionsDiv.querySelector('[data-action="delete-msg"]')?.addEventListener('click', (e) => {
                e.stopPropagation();
                const menu = actionsDiv.querySelector('.chat-msg-menu');
                if (menu) menu.classList.remove('show');
                this._deleteChatMessage(msgId);
            });
        }

        if (!div._menuClickListener) {
            div._menuClickListener = (e) => {
                if (!e.target.closest('.chat-msg-menu') && !e.target.closest('[data-action="menu"]')) {
                    div.querySelectorAll('.chat-msg-menu.show').forEach(m => m.classList.remove('show'));
                }
            };
            document.addEventListener('click', div._menuClickListener);
        }
    }

    _renderChatRange() {
        const container = this._id(this._opts.messagesId);
        if (!container) return;
        const msgs = APP.state.persistedChatMessages;
        const showCount = Math.min(APP.state._chatVisibleCount, msgs.length);
        const startIdx = Math.max(0, msgs.length - showCount);

        container.innerHTML = '';

        if (startIdx > 0) {
            const loadMore = document.createElement('div');
            loadMore.className = 'load-more-btn';
            loadMore.textContent = '📜 載入更多 (' + startIdx + ' 條較早訊息)';
            loadMore.style.cssText = 'text-align:center;padding:8px;cursor:pointer;color:#38bdf8;font-size:13px;border-bottom:1px solid #1f2937;';
            loadMore.onclick = () => {
                APP.state._chatVisibleCount = Math.min(APP.state._chatVisibleCount + this._pageSize, msgs.length);
                this._renderChatRange();
                container.scrollTop = 0;
            };
            container.appendChild(loadMore);
        }

        for (let i = startIdx; i < msgs.length; i++) {
            const msg = msgs[i];
            this._renderSingleChatMessage(msg.from, msg.text, msg.timestamp, msg.msgId, msg.replyTo, container);
        }
    }

    _renderChatMessage(from, text, timestamp, msgId, replyTo) {
        this._renderChatRange();
        requestAnimationFrame(() => {
            const cm = this._id(this._opts.messagesId);
            if (cm) cm.scrollTop = cm.scrollHeight;
        });
    }

    // ── Edit / Delete ──

    _editChatMessage(msgId) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        if (!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }
        const el = document.querySelector('[data-msgid="' + msgId + '"]');
        if (!el) return;
        const textDiv = el.querySelector('.msg-text');
        if (!textDiv) return;
        const currentText = textDiv.textContent;
        const newText = prompt('編輯訊息：', currentText);
        if (newText === null || newText.trim() === '' || newText.trim() === currentText) return;
        const trimmed = newText.trim();
        textDiv.textContent = trimmed;
        let editedLabel = el.querySelector('.msg-edited');
        if (!editedLabel) {
            editedLabel = document.createElement('span');
            editedLabel.className = 'msg-edited';
            el.querySelector('.msg-time')?.appendChild(editedLabel);
        }
        editedLabel.textContent = ' (已編輯)';
        broadcastToPeers(JSON.stringify({ type: 'chat-edit', msgId, newText: trimmed }));
        APP.showStatusMsg('✅ 訊息已編輯');
    }

    async _deleteChatMessage(msgId) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        if (!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }
        const confirmed = await showConfirmDialog('確定刪除此訊息？');
        if (!confirmed) return;
        const el = document.querySelector('[data-msgid="' + msgId + '"]');
        if (!el) return;
        el.classList.add('deleted');
        el.innerHTML = '<div class="msg-text" style="text-align:center;font-style:italic;color:#64748b;">此訊息已刪除</div>';
        broadcastToPeers(JSON.stringify({ type: 'chat-delete', msgId }));
        APP.showStatusMsg('✅ 訊息已刪除');
    }

    // ── Search ──

    _searchChat(query) {
        const container = this._id(this._opts.messagesId);
        if (!container) return;
        const msgs = container.querySelectorAll('.chat-msg');
        if (!query || !query.trim()) {
            msgs.forEach(el => el.style.display = '');
            return;
        }
        const q = query.trim().toLowerCase();
        msgs.forEach(el => {
            const text = (el.textContent || '').toLowerCase();
            el.style.display = text.includes(q) ? '' : 'none';
        });
    }

    // ── Typing indicators ──

    _showTyping(fromName) {
        const typingEl = this._id(this._opts.typingIndicatorId);
        if (!typingEl) return;
        typingEl.textContent = '👤 ' + fromName + ' 正在輸入⋯';
        typingEl.style.display = '';
        clearTimeout(typingEl._hideTimer);
        typingEl._hideTimer = setTimeout(() => { typingEl.style.display = 'none'; }, 3000);
    }

    _hideTyping() {
        const typingEl = this._id(this._opts.typingIndicatorId);
        if (!typingEl) return;
        typingEl.style.display = 'none';
        clearTimeout(typingEl._hideTimer);
    }

    _cleanupTyping() {
        if (this._typingTimer) { clearTimeout(this._typingTimer); this._typingTimer = null; }
        if (this._typingStopTimer) { clearTimeout(this._typingStopTimer); this._typingStopTimer = null; }
    }

    // ── Event wiring ──

    _wireEvents() {
        this._sendHandler = () => this._sendChatMessage();
        this._id(this._opts.sendBtnId)?.addEventListener('click', this._sendHandler);

        this._inputKeyHandler = (e) => {
            if (e.key === 'Enter') this._sendChatMessage();
        };
        this._id(this._opts.inputId)?.addEventListener('keydown', this._inputKeyHandler);

        this._clearHandler = async () => {
            const confirmed = await showConfirmDialog('確定要清除本機聊天紀錄嗎？此操作僅影響本機，不會影響其他客戶端。');
            if (!confirmed) return;
            APP.state.persistedChatMessages = [];
            APP.state._chatVisibleCount = 15;
            saveToStorage('vcc_chat_messages', []);
            const cm = this._id(this._opts.messagesId);
            if (cm) cm.innerHTML = '';
            APP.showStatusMsg('本機聊天紀錄已清除');
        };
        this._id(this._opts.clearBtnId)?.addEventListener('click', this._clearHandler);

        this._searchInputHandler = () => this._searchChat(this._id(this._opts.searchId)?.value);
        this._id(this._opts.searchId)?.addEventListener('input', this._searchInputHandler);

        this._searchKeyHandler = (e) => {
            if (e.key === 'Enter') this._searchChat(this._id(this._opts.searchId)?.value);
        };
        this._id(this._opts.searchId)?.addEventListener('keydown', this._searchKeyHandler);

        this._replyCloseHandler = () => {
            APP.state._pendingReply = null;
            const bar = this._id(this._opts.replyBarId);
            if (bar) bar.style.display = 'none';
        };
        this._id(this._opts.replyCloseId)?.addEventListener('click', this._replyCloseHandler);

        this._typingInputHandler = () => {
            if (!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN || !APP.state.room) return;
            this._cleanupTyping();
            this._typingTimer = setTimeout(() => {
                broadcastToPeers(JSON.stringify({ type: 'typing', from: APP.state.displayName }));
                this._typingTimer = null;
                this._typingStopTimer = setTimeout(() => {
                    broadcastToPeers(JSON.stringify({ type: 'typing-stop' }));
                    this._typingStopTimer = null;
                }, 2000);
            }, 300);
        };
        this._id(this._opts.inputId)?.addEventListener('input', this._typingInputHandler);
    }

    _removeEvents() {
        const b = this._id(this._opts.sendBtnId);
        if (b && this._sendHandler) b.removeEventListener('click', this._sendHandler);
        const inp = this._id(this._opts.inputId);
        if (inp && this._inputKeyHandler) inp.removeEventListener('keydown', this._inputKeyHandler);
        if (inp && this._typingInputHandler) inp.removeEventListener('input', this._typingInputHandler);
        const clr = this._id(this._opts.clearBtnId);
        if (clr && this._clearHandler) clr.removeEventListener('click', this._clearHandler);
        const sch = this._id(this._opts.searchId);
        if (sch && this._searchInputHandler) sch.removeEventListener('input', this._searchInputHandler);
        if (sch && this._searchKeyHandler) sch.removeEventListener('keydown', this._searchKeyHandler);
        const rcl = this._id(this._opts.replyCloseId);
        if (rcl && this._replyCloseHandler) rcl.removeEventListener('click', this._replyCloseHandler);
    }

    // ── Helpers ──

    _id(id) {
        return document.getElementById(id);
    }
}
