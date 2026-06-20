/**
 * NoticeModule — 公告欄模組
 *
 * Depends on: ClipperModule (js/core/module-base.js), globals in clipper.html
 *   (APP, saveToStorage, loadFromStorage, sendWsMessage, showPopup, showConfirmDialog, escapeHtml)
 *
 * WS messages handled: notice-create, notice-edit, notice-delete, notice-pin, room-state
 * Backward compat: reads/writes APP.state.noticePosts for interoperability
 */
class NoticeModule extends ClipperModule {
    constructor(bus, wsManager) {
        super('notice', bus, wsManager);
        this.NOTICE_CATEGORIES = [
            { value: '',         label: '無分類', color: '#64748b' },
            { value: '重要',     label: '重要',   color: '#ef4444' },
            { value: '日常',     label: '日常',   color: '#38bdf8' },
            { value: '技術',     label: '技術',   color: '#22c55e' },
            { value: '其他事項', label: '其他事項', color: '#64748b' },
        ];

        wsManager.onMessage(
            ['notice-create', 'notice-edit', 'notice-delete', 'notice-pin', 'room-state'],
            (data) => this.handleServerMessage(data),
            'notice'
        );
    }

    _mount() {
        const posts = loadFromStorage(APP.roomKey('vcc_notice_posts'), []);
        APP.state.noticePosts = posts;
        this.renderNoticeBoard();
    }

    _unmount() {
        // cleanup handled by bus/wsManager lifecycle
    }

    handleServerMessage(data) {
        try {
            switch (data.type) {
                case 'notice-create':
                    this.mergeNoticePosts([data.post]);
                    saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
                    this.renderNoticeBoard();
                    showPopup('📋', '新公告', (data.post && data.post.author ? data.post.author + '：' : '') + (data.post && data.post.title ? data.post.title : ''));
                    break;

                case 'notice-edit': {
                    const editPost = APP.state.noticePosts.find(p => p.id === data.id);
                    if (editPost) {
                        editPost.title = data.title;
                        editPost.content = data.content;
                        editPost.editedAt = data.editedAt;
                        if (data.category !== undefined) editPost.category = data.category;
                        if (data.tags !== undefined) editPost.tags = data.tags;
                        if (data.color !== undefined) editPost.color = data.color;
                        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
                        this.renderNoticeBoard();
                    }
                    break;
                }

                case 'notice-delete':
                    APP.state.noticePosts = APP.state.noticePosts.filter(p => p.id !== data.id);
                    saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
                    this.renderNoticeBoard();
                    break;

                case 'notice-pin': {
                    const pinPost = APP.state.noticePosts.find(p => p.id === data.id);
                    if (pinPost) {
                        pinPost.pinned = data.pinned;
                        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
                        this.renderNoticeBoard();
                    }
                    break;
                }

                case 'room-state':
                    // Room-state merge logic stays in clipper.html legacy handler;
                    // just re-render to reflect any changes
                    this.renderNoticeBoard();
                    break;
            }
        } catch (err) {
            console.error(`[Module:notice] handleServerMessage error:`, err);
            this.bus.emit('module-error', { module: this.name, error: err, type: data.type });
        }
    }

    getCategoryColor(cat) {
        const found = this.NOTICE_CATEGORIES.find(c => c.value === cat);
        return found ? found.color : '#64748b';
    }

    mergeNoticePosts(incoming) {
        const existing = APP.state.noticePosts;
        for (const post of incoming) {
            const idx = existing.findIndex(p => p.id === post.id);
            if (idx >= 0) {
                if (post.timestamp > existing[idx].timestamp) {
                    existing[idx] = post;
                }
            } else {
                existing.push(post);
            }
        }
        APP.state.noticePosts = existing;
    }

    renderNoticeBoard() {
        this.renderCmsTable();

        const sidebarList = document.getElementById('noticeListSidebar');
        if (sidebarList) {
            const sorted = [...APP.state.noticePosts].sort((a, b) => {
                if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
                return b.timestamp - a.timestamp;
            });
            if (sorted.length === 0) {
                sidebarList.innerHTML = '<div style="color:#64748b;text-align:center;padding:20px;font-size:14px">暫無公告</div>';
                return;
            }
            sidebarList.innerHTML = sorted.map(post => {
                const timeStr = new Date(post.timestamp).toLocaleString('zh-TW', {hour:'2-digit',minute:'2-digit'});
                const catColor = this.getCategoryColor(post.category || '');
                const catLabel = this.NOTICE_CATEGORIES.find(c => c.value === (post.category || ''))?.label || '';
                const categoryBadge = catLabel ? '<span class="category-badge" style="background:' + catColor + ';font-size:10px;padding:1px 6px;">' + escapeHtml(catLabel) + '</span>' : '';
                const tagsHtml = (post.tags && post.tags.length) ? post.tags.slice(0,2).map(t => '<span class="tag-badge" style="font-size:9px;">' + escapeHtml(t) + '</span>').join('') : '';
                return '<div class="notice-card' + (post.pinned ? ' pinned' : '') + '" style="border-left-color:' + catColor + ';padding:10px 12px;">'
                    + '<div class="notice-card-header" style="margin-bottom:4px;">'
                    + '<div class="notice-card-title" style="font-size:18px;">' + categoryBadge + escapeHtml(post.title) + '</div>'
                    + (post.pinned ? '<span class="pin-badge" style="font-size:12px;">📌</span>' : '')
                    + '</div>'
                    + '<div class="notice-card-meta" style="font-size:11px;margin-bottom:2px;">' + escapeHtml(post.author) + ' · ' + timeStr + '</div>'
                    + (tagsHtml ? '<div class="notice-card-meta">' + tagsHtml + '</div>' : '')
                    + '<div class="notice-card-content">' + post.content + '</div>'
                    + '</div>';
            }).join('');
        }
    }

    getCmsFilteredPosts() {
        const search = (document.getElementById('cmsSearch')?.value || '').toLowerCase();
        const catFilter = document.getElementById('cmsFilterCat')?.value || '';
        let filtered = APP.state.noticePosts;
        if (catFilter) filtered = filtered.filter(p => p.category === catFilter);
        if (search) filtered = filtered.filter(p =>
            (p.title || '').toLowerCase().includes(search) ||
            (p.content || '').toLowerCase().includes(search) ||
            (p.author || '').toLowerCase().includes(search) ||
            (p.tags || []).some(t => t.toLowerCase().includes(search))
        );
        return filtered.sort((a, b) => {
            if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
            return b.timestamp - a.timestamp;
        });
    }

    renderCmsTable() {
        const tbody = document.getElementById('cmsTbody');
        const empty = document.getElementById('cmsEmpty');
        const table = document.getElementById('cmsTable');
        if (!tbody) { this.renderNoticeBoardSidebar(); return; }

        const total = APP.state.noticePosts.length;
        const pinned = APP.state.noticePosts.filter(p => p.pinned).length;
        const important = APP.state.noticePosts.filter(p => p.category === '重要').length;
        const totalEl = document.getElementById('cmsTotal');
        const pinnedEl = document.getElementById('cmsPinned');
        const importantEl = document.getElementById('cmsImportant');
        if (totalEl) totalEl.textContent = total;
        if (pinnedEl) pinnedEl.textContent = pinned;
        if (importantEl) importantEl.textContent = important;

        const filtered = this.getCmsFilteredPosts();
        const hasData = filtered.length > 0;
        tbody.style.display = hasData ? '' : 'none';
        empty.style.display = hasData ? 'none' : '';
        if (!hasData) return;

        tbody.innerHTML = filtered.map(post => {
            const catColor = this.getCategoryColor(post.category || '');
            const catLabel = this.NOTICE_CATEGORIES.find(c => c.value === (post.category || ''))?.label || '';
            const catPill = catLabel ? '<span class="cms-cat-pill" style="background:' + catColor + '">' + escapeHtml(catLabel) + '</span>' : '<span style="color:#475569;">—</span>';
            const tagsHtml = (post.tags && post.tags.length) ? post.tags.map(t => '<span class="cms-tag">' + escapeHtml(t) + '</span>').join('') : '<span style="color:#475569;">—</span>';
            const timeStr = new Date(post.timestamp).toLocaleString('zh-TW', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
            const pinnedMark = post.pinned ? '📌' : '';
            return '<tr class="' + (post.pinned ? 'tr-pinned' : '') + '">'
                + '<td class="cms-col-pin">' + pinnedMark + '</td>'
                + '<td class="cms-col-cat">' + catPill + '</td>'
                + '<td class="cms-col-title"><span class="cms-post-title" data-id="' + post.id + '" title="' + escapeHtml(post.content) + '">' + escapeHtml(post.title) + '</span></td>'
                + '<td class="cms-col-tags">' + tagsHtml + '</td>'
                + '<td class="cms-col-author">' + escapeHtml(post.author || '—') + '</td>'
                + '<td class="cms-col-time" style="color:#64748b;">' + timeStr + '</td>'
                + '<td class="cms-col-actions">'
                + '<button class="cms-action-btn" data-action="edit" data-id="' + post.id + '" title="編輯">✏️</button>'
                + '<button class="cms-action-btn" data-action="pin" data-id="' + post.id + '" title="' + (post.pinned ? '取消置頂' : '置頂') + '">📌</button>'
                + '<button class="cms-action-btn del" data-action="delete" data-id="' + post.id + '" title="刪除">🗑</button>'
                + '</td></tr>';
        }).join('');

        tbody.querySelectorAll('.cms-post-title').forEach(el => {
            el.addEventListener('click', () => {
                const post = APP.state.noticePosts.find(p => p.id === el.dataset.id);
                if (post) this.showNoticeForm(post);
            });
        });
        tbody.querySelectorAll('[data-action="edit"]').forEach(btn => {
            btn.addEventListener('click', () => {
                const post = APP.state.noticePosts.find(p => p.id === btn.dataset.id);
                if (post) this.showNoticeForm(post);
            });
        });
        tbody.querySelectorAll('[data-action="pin"]').forEach(btn => {
            btn.addEventListener('click', () => this.togglePinNoticePost(btn.dataset.id));
        });
        tbody.querySelectorAll('[data-action="delete"]').forEach(btn => {
            btn.addEventListener('click', () => this.deleteNoticePost(btn.dataset.id));
        });
    }

    renderNoticeBoardSidebar() {
        const sidebarList = document.getElementById('noticeListSidebar');
        if (!sidebarList) return;
        const sorted = [...APP.state.noticePosts].sort((a, b) => {
            if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
            return b.timestamp - a.timestamp;
        });
        if (sorted.length === 0) {
            sidebarList.innerHTML = '<div style="color:#64748b;text-align:center;padding:20px;">暫無公告</div>';
            return;
        }
        sidebarList.innerHTML = sorted.map(post => {
            const timeStr = new Date(post.timestamp).toLocaleString('zh-TW', {hour:'2-digit',minute:'2-digit'});
            const catColor = this.getCategoryColor(post.category || '');
            const catLabel = this.NOTICE_CATEGORIES.find(c => c.value === (post.category || ''))?.label || '';
            const badge = catLabel ? '<span class="category-badge" style="background:' + catColor + ';font-size:10px;padding:1px 6px;">' + escapeHtml(catLabel) + '</span>' : '';
            return '<div class="notice-card' + (post.pinned ? ' pinned' : '') + '" style="border-left-color:' + catColor + ';padding:10px 12px;">'
                + '<div class="notice-card-header" style="margin-bottom:4px;">'
                + '<div class="notice-card-title" style="font-size:18px;">' + badge + escapeHtml(post.title) + '</div>'
                + (post.pinned ? '<span style="font-size:12px;">📌</span>' : '')
                + '</div>'
                + '<div style="font-size:11px;color:#64748b;">' + escapeHtml(post.author) + ' · ' + timeStr + '</div>'
                + '<div class="notice-card-content">' + post.content + '</div>'
                + '</div>';
        }).join('');
    }

    showNoticeForm(post) {
        const existingOverlay = document.querySelector('.notice-post-form');
        if (existingOverlay) existingOverlay.remove();

        const overlay = document.createElement('div');
        overlay.className = 'notice-post-form';
        const isEdit = !!post;
        const catOptions = this.NOTICE_CATEGORIES.map(c =>
            '<option value="' + c.value + '"' + ((isEdit && post.category === c.value) ? ' selected' : '') + '>' + c.label + '</option>'
        ).join('');
        const tagsVal = (isEdit && post.tags) ? post.tags.join(',') : '';
        overlay.innerHTML = '<div class="notice-form-box">'
            + '<h3>' + (isEdit ? '✏️ 編輯公告' : '✏️ 新增公告') + '</h3>'
            + '<input type="text" class="notice-form-input" id="noticeFormTitle" placeholder="公告標題" value="' + (isEdit ? escapeHtml(post.title) : '') + '">'
            + '<select class="notice-form-input" id="noticeFormCategory">' + catOptions + '</select>'
            + '<input type="text" class="notice-form-input" id="noticeFormTags" placeholder="標籤，逗號分隔" value="' + escapeHtml(tagsVal) + '">'
            + '<div class="fmt-toolbar" id="fmtToolbar">'
            + '<button type="button" class="fmt-btn" data-fmt="bold" title="粗體"><b>B</b></button>'
            + '<button type="button" class="fmt-btn" data-fmt="italic" title="斜體"><i>I</i></button>'
            + '<button type="button" class="fmt-btn" data-fmt="strikeThrough" title="刪除線"><s>S</s></button>'
            + '</div>'
            + '<textarea class="notice-form-input notice-form-textarea" id="noticeFormContent" placeholder="公告內容...">' + (isEdit ? post.content : '') + '</textarea>'
            + '<div class="notice-form-buttons">'
            + '<button class="btn btn-secondary" id="noticeFormCancel">取消</button>'
            + '<button class="btn btn-primary" id="noticeFormSave">' + (isEdit ? '儲存' : '發布') + '</button>'
            + '</div></div>';
        document.body.appendChild(overlay);

        const titleInput = document.getElementById('noticeFormTitle');
        const categoryInput = document.getElementById('noticeFormCategory');
        const tagsInput = document.getElementById('noticeFormTags');
        const contentInput = document.getElementById('noticeFormContent');

        overlay.querySelectorAll('.fmt-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const ta = contentInput;
                const start = ta.selectionStart;
                const end = ta.selectionEnd;
                if (start === undefined || start === end) {
                    APP.showStatusMsg('💡 請先選取要格式化的文字');
                    return;
                }
                const fmt = btn.dataset.fmt;
                let tag;
                if (fmt === 'bold') tag = 'b';
                else if (fmt === 'italic') tag = 'i';
                else if (fmt === 'strikeThrough') tag = 's';
                const selected = ta.value.substring(start, end);
                const before = ta.value.substring(0, start);
                const after = ta.value.substring(end);
                ta.value = before + '<' + tag + '>' + selected + '</' + tag + '>' + after;
                const newEnd = start + selected.length + 3 + tag.length * 2 + 5;
                ta.setSelectionRange(newEnd, newEnd);
                ta.focus();
            });
        });

        document.getElementById('noticeFormCancel').addEventListener('click', () => overlay.remove());
        document.getElementById('noticeFormSave').addEventListener('click', () => {
            const title = titleInput.value.trim();
            const content = contentInput.value.trim();
            if (!title || !content) {
                APP.showStatusMsg('❌ 標題和內容不能為空');
                return;
            }
            const category = categoryInput.value;
            const tagsString = tagsInput.value;
            if (isEdit) {
                this.editNoticePost(post.id, title, content, category, tagsString);
            } else {
                this.createNoticePost(title, content, category, tagsString);
            }
            overlay.remove();
        });
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) overlay.remove();
        });
        titleInput.focus();
    }

    createNoticePost(title, content, category, tagsString) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        if ((!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) || !APP.state.room) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }
        const tags = tagsString ? tagsString.split(',').map(t => t.trim()).filter(Boolean) : [];
        const color = this.getCategoryColor(category || '');
        const newPost = {
            id: crypto.randomUUID(),
            title: title.trim(),
            content: content.trim(),
            author: APP.state.displayName,
            pinned: false,
            timestamp: Date.now(),
            editedAt: null,
            category: category || '',
            tags: tags,
            color: color
        };
        APP.state.noticePosts.push(newPost);
        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
        sendWsMessage({type: 'notice-create', room: APP.state.room, post: newPost});
        this.renderNoticeBoard();
        APP.showStatusMsg('✅ 公告已發布');
    }

    editNoticePost(id, title, content, category, tagsString) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        if ((!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) || !APP.state.room) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }
        const post = APP.state.noticePosts.find(p => p.id === id);
        if (!post) return;
        const tags = tagsString ? tagsString.split(',').map(t => t.trim()).filter(Boolean) : [];
        const color = this.getCategoryColor(category || '');
        post.title = title.trim();
        post.content = content.trim();
        post.category = category || '';
        post.tags = tags;
        post.color = color;
        post.editedAt = Date.now();
        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
        sendWsMessage({type: 'notice-edit', room: APP.state.room, id, title: post.title, content: post.content, category: post.category, tags: post.tags, color: post.color, editedAt: post.editedAt});
        this.renderNoticeBoard();
        APP.showStatusMsg('✅ 公告已更新');
    }

    async deleteNoticePost(id) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        const post = APP.state.noticePosts.find(p => p.id === id);
        if (!post) { APP.showStatusMsg('❌ 找不到該公告'); return; }
        let msg = '確定刪除此公告？';
        if (post.category === '重要') {
            msg = '⚠️ 此為【重要】公告！' + msg;
        }
        if (!await showConfirmDialog(msg)) return;
        APP.state.noticePosts = APP.state.noticePosts.filter(p => p.id !== id);
        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
        sendWsMessage({type: 'notice-delete', room: APP.state.room, id});
        this.renderNoticeBoard();
        APP.showStatusMsg('✅ 公告已刪除');
    }

    togglePinNoticePost(id) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        const post = APP.state.noticePosts.find(p => p.id === id);
        if (!post) return;
        post.pinned = !post.pinned;
        saveToStorage(APP.roomKey('vcc_notice_posts'), APP.state.noticePosts);
        sendWsMessage({type: 'notice-pin', room: APP.state.room, id, pinned: post.pinned});
        this.renderNoticeBoard();
    }
}
