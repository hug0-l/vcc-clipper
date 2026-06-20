/**
 * TemplatesModule — checklist template manager (範本庫)
 *
 * Extends ClipperModule. Handles creating, editing, deleting, and applying
 * checklist templates from the templates pane.
 *
 * Depends on: ClipperModule (js/core/module-base.js), MessageBus, WSManager
 * Global deps: window.APP, window.saveToStorage, window.loadFromStorage,
 *   window.sendWsMessage, window.showConfirmDialog, window.escapeHtml,
 *   window.getCategoryColor, window.renderChecklistBoards
 */
class TemplatesModule extends ClipperModule {
    constructor(bus, wsManager) {
        super('templates', bus, wsManager);
        this._boundInputHandler = null;
        this._boundKeydownHandler = null;
        this._boundSearchHandler = null;
        this._boundCatHandler = null;
        this._boundNewHandler = null;
    }

    _mount() {
        // Load templates state
        this._templates = APP.state.checklistTemplates || [];

        // Render
        this.renderTemplates();

        // Bind event listeners
        this._bindEvents();
    }

    _unmount() {
        this._unbindEvents();
    }

    _bindEvents() {
        const searchInput = document.getElementById('tplSearch');
        const searchBtn = document.getElementById('btnTplSearch');
        const catFilter = document.getElementById('tplFilterCat');
        const newBtn = document.getElementById('btnNewTemplate');

        this._boundInputHandler = () => this.renderTemplates();
        this._boundKeydownHandler = (e) => { if (e.key === 'Enter') this.renderTemplates(); };
        this._boundSearchHandler = () => this.renderTemplates();
        this._boundCatHandler = () => this.renderTemplates();
        this._boundNewHandler = () => this.showTemplateForm(null);

        if (searchInput) {
            searchInput.addEventListener('input', this._boundInputHandler);
            searchInput.addEventListener('keydown', this._boundKeydownHandler);
        }
        if (searchBtn) searchBtn.addEventListener('click', this._boundSearchHandler);
        if (catFilter) catFilter.addEventListener('change', this._boundCatHandler);
        if (newBtn) newBtn.addEventListener('click', this._boundNewHandler);
    }

    _unbindEvents() {
        const searchInput = document.getElementById('tplSearch');
        const searchBtn = document.getElementById('btnTplSearch');
        const catFilter = document.getElementById('tplFilterCat');
        const newBtn = document.getElementById('btnNewTemplate');

        if (searchInput && this._boundInputHandler) {
            searchInput.removeEventListener('input', this._boundInputHandler);
        }
        if (searchInput && this._boundKeydownHandler) {
            searchInput.removeEventListener('keydown', this._boundKeydownHandler);
        }
        if (searchBtn && this._boundSearchHandler) {
            searchBtn.removeEventListener('click', this._boundSearchHandler);
        }
        if (catFilter && this._boundCatHandler) {
            catFilter.removeEventListener('change', this._boundCatHandler);
        }
        if (newBtn && this._boundNewHandler) {
            newBtn.removeEventListener('click', this._boundNewHandler);
        }
    }

    renderTemplates() {
        const grid = document.getElementById('tplGrid');
        if (!grid) return;
        const templates = APP.state.checklistTemplates;
        document.getElementById('tplTotal').textContent = templates.length;
        const cats = new Set(templates.filter(t => t.category).map(t => t.category));
        document.getElementById('tplCategories').textContent = cats.size;

        // Filter
        const search = (document.getElementById('tplSearch')?.value || '').toLowerCase();
        const catFilter = document.getElementById('tplFilterCat')?.value || '';
        let filtered = templates;
        if (catFilter) filtered = filtered.filter(t => t.category === catFilter);
        if (search) filtered = filtered.filter(t =>
            (t.title || '').toLowerCase().includes(search) ||
            (t.description || '').toLowerCase().includes(search) ||
            (t.tags || []).some(tg => tg.toLowerCase().includes(search))
        );

        if (filtered.length === 0) {
            grid.innerHTML = '<div class="tpl-empty">📁 ' + (templates.length === 0 ? '尚無範本，建立第一個範本以便快速部署檢查表' : '無符合條件的範本') + '</div>';
            return;
        }
        grid.innerHTML = filtered.map(tpl => {
            const catColor = getCategoryColor(tpl.category || '') || '#38bdf8';
            const catLabel = tpl.category || '未分類';
            const itemCount = (tpl.items || []).length;
            return '<div class="tpl-card" style="border-left-color:' + catColor + '">'
                + '<div class="tpl-card-icon">📋</div>'
                + '<div class="tpl-card-info">'
                + '<div class="tpl-card-title">' + escapeHtml(tpl.title) + '</div>'
                + '<div class="tpl-card-meta">' + escapeHtml(catLabel) + ' · ' + itemCount + ' 個項目 · ' + escapeHtml(tpl.createdBy) + '</div>'
                + (tpl.description ? '<div class="tpl-card-desc">' + escapeHtml(tpl.description) + '</div>' : '')
                + '</div>'
                + '<div class="tpl-card-actions">'
                + '<button class="use-btn" data-action="use" data-id="' + tpl.id + '" title="建立副本">📋 使用</button>'
                + '<button data-action="edit" data-id="' + tpl.id + '" title="編輯">✏️</button>'
                + '<button data-action="delete" data-id="' + tpl.id + '" title="刪除" style="color:#ef4444;">🗑</button>'
                + '</div></div>';
        }).join('');

        grid.querySelectorAll('[data-action="use"]').forEach(btn => {
            btn.addEventListener('click', () => this.useTemplate(btn.dataset.id));
        });
        grid.querySelectorAll('[data-action="edit"]').forEach(btn => {
            btn.addEventListener('click', () => this.showTemplateForm(btn.dataset.id));
        });
        grid.querySelectorAll('[data-action="delete"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                if (!await showConfirmDialog('確定刪除此範本？')) return;
                APP.state.checklistTemplates = APP.state.checklistTemplates.filter(t => t.id !== btn.dataset.id);
                saveToStorage(APP.roomKey('vcc_checklist_templates'), APP.state.checklistTemplates);
                this.renderTemplates();
            });
        });
    }

    useTemplate(tplId) {
        if (APP.state.readOnly) {
            APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
            return;
        }
        const tpl = APP.state.checklistTemplates.find(t => t.id === tplId);
        if (!tpl) { APP.showStatusMsg('❌ 找不到該範本'); return; }
        if ((!APP.state.ws || APP.state.ws.readyState !== WebSocket.OPEN) || !APP.state.room) {
            APP.showStatusMsg('❌ 請先建立連線');
            return;
        }
        const newBoard = {
            id: crypto.randomUUID(),
            title: tpl.title + ' (副本)',
            category: tpl.category || '',
            tags: [...(tpl.tags || [])],
            color: tpl.color || '#38bdf8',
            pinned: false,
            createdBy: APP.state.displayName,
            createdAt: Date.now(),
            items: (tpl.items || []).map(item => ({
                id: crypto.randomUUID(),
                text: typeof item === 'string' ? item : (item.text || ''),
                checked: false,
                addedBy: APP.state.displayName,
                timestamp: Date.now(),
                checkedAt: null
            }))
        };
        APP.state.checklists.push(newBoard);
        saveToStorage(APP.roomKey('vcc_checklists'), APP.state.checklists);
        sendWsMessage({type: 'checklistboard-create', room: APP.state.room, board: newBoard});
        window.renderChecklistBoards();
        // Switch to active tab
        document.querySelector('[data-cltab="active"]')?.click();
        APP.showStatusMsg('✅ 已從範本建立「' + newBoard.title + '」');
    }

    showTemplateForm(tplId) {
        const existing = document.querySelector('.tpl-form-overlay');
        if (existing) existing.remove();
        const isEdit = !!tplId;
        const tpl = isEdit ? APP.state.checklistTemplates.find(t => t.id === tplId) : null;
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay tpl-form-overlay';
        const itemsHtml = tpl ? (tpl.items || []).map((item, i) =>
            '<div style="display:flex;gap:6px;margin-bottom:4px;"><input class="tpl-item-input" id="tplItem_' + i + '" value="' + escapeHtml(typeof item === 'string' ? item : (item.text || '')) + '" style="flex:1;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:6px 10px;border-radius:6px;font-size:14px;outline:none;font-family:inherit;"><button class="btn-icon tpl-remove-item" style="color:#ef4444;font-size:14px;">✕</button></div>'
        ).join('') : '';
        const itemCount = tpl ? (tpl.items || []).length : 0;
        overlay.innerHTML = '<div class="modal-dialog" style="max-width:480px;">'
            + '<h3 style="font-size:20px;font-weight:600;margin-bottom:12px;">' + (isEdit ? '✏️ 編輯範本' : '📁 新增範本') + '</h3>'
            + '<input type="text" class="notice-form-input" id="tplFormTitle" placeholder="範本名稱（必填）" value="' + (tpl ? escapeHtml(tpl.title) : '') + '">'
            + '<input type="text" class="notice-form-input" id="tplFormDesc" placeholder="描述（選填）" value="' + (tpl ? escapeHtml(tpl.description || '') : '') + '">'
            + '<select class="notice-form-input" id="tplFormCat">'
            + '<option value="">無分類</option>'
            + '<option value="每日檢查"' + (isEdit && tpl.category === '每日檢查' ? ' selected' : '') + '>每日檢查</option>'
            + '<option value="每週檢查"' + (isEdit && tpl.category === '每週檢查' ? ' selected' : '') + '>每週檢查</option>'
            + '<option value="每月檢查"' + (isEdit && tpl.category === '每月檢查' ? ' selected' : '') + '>每月檢查</option>'
            + '<option value="事故應變"' + (isEdit && tpl.category === '事故應變' ? ' selected' : '') + '>事故應變</option>'
            + '<option value="交接事項"' + (isEdit && tpl.category === '交接事項' ? ' selected' : '') + '>交接事項</option>'
            + '<option value="其他"' + (isEdit && tpl.category === '其他' ? ' selected' : '') + '>其他</option>'
            + '</select>'
            + '<input type="text" class="notice-form-input" id="tplFormTags" placeholder="標籤，逗號分隔" value="' + (isEdit && tpl.tags ? escapeHtml(tpl.tags.join(',')) : '') + '">'
            + '<div style="font-size:15px;color:#94a3b8;margin-bottom:6px;">項目清單 <span style="font-size:12px;color:#475569;">（每個項目一行）</span></div>'
            + '<div id="tplItemsContainer" style="margin-bottom:8px;">' + itemsHtml + '</div>'
            + (itemCount === 0 ? '<div style="color:#475569;font-size:13px;margin-bottom:8px;" id="tplNoItems">尚未新增項目</div>' : '')
            + '<button class="btn btn-secondary" id="tplAddItem" style="font-size:12px;padding:4px 10px;margin-bottom:12px;">➕ 新增項目</button>'
            + '<div class="notice-form-buttons">'
            + '<button class="btn btn-secondary" id="tplFormCancel">取消</button>'
            + '<button class="btn btn-primary" id="tplFormSave">' + (isEdit ? '儲存' : '建立') + '</button>'
            + '</div></div>';
        document.body.appendChild(overlay);

        const self = this;
        // Add item button
        let itemIdx = isEdit ? (tpl.items || []).length : 0;
        document.getElementById('tplAddItem').addEventListener('click', () => {
            const container = document.getElementById('tplItemsContainer');
            const noItems = document.getElementById('tplNoItems');
            if (noItems) noItems.remove();
            const div = document.createElement('div');
            div.style.cssText = 'display:flex;gap:6px;margin-bottom:4px;';
            div.innerHTML = '<input class="tpl-item-input" id="tplItem_' + itemIdx + '" style="flex:1;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:6px 10px;border-radius:6px;font-size:14px;outline:none;font-family:inherit;" placeholder="輸入項目..."><button class="btn-icon tpl-remove-item" style="color:#ef4444;font-size:14px;">✕</button>';
            container.appendChild(div);
            div.querySelector('.tpl-remove-item').addEventListener('click', () => { div.remove(); });
            document.getElementById('tplItem_' + itemIdx).focus();
            itemIdx++;
        });
        // Remove item buttons
        overlay.querySelectorAll('.tpl-remove-item').forEach(btn => {
            btn.addEventListener('click', function() { this.parentElement.remove(); });
        });

        document.getElementById('tplFormCancel').addEventListener('click', () => overlay.remove());
        document.getElementById('tplFormSave').addEventListener('click', function() {
            if (APP.state.readOnly) {
                APP.showStatusMsg('🔒 伺服器中斷，唯讀模式不可操作');
                return;
            }
            const title = document.getElementById('tplFormTitle').value.trim();
            if (!title) { APP.showStatusMsg('❌ 範本名稱不能為空'); return; }
            const desc = document.getElementById('tplFormDesc').value.trim();
            const cat = document.getElementById('tplFormCat').value;
            const tagsStr = document.getElementById('tplFormTags').value.trim();
            const tags = tagsStr ? tagsStr.split(',').map(t => t.trim()).filter(Boolean) : [];
            const itemInputs = overlay.querySelectorAll('.tpl-item-input');
            const items = [];
            itemInputs.forEach(inp => {
                const v = inp.value.trim();
                if (v) items.push({text: v});
            });
            if (isEdit) {
                const existing = APP.state.checklistTemplates.find(t => t.id === tplId);
                if (existing) {
                    existing.title = title;
                    existing.description = desc;
                    existing.category = cat;
                    existing.tags = tags;
                    existing.items = items;
                    existing.editedAt = Date.now();
                }
            } else {
                APP.state.checklistTemplates.push({
                    id: crypto.randomUUID(),
                    title,
                    description: desc,
                    category: cat,
                    tags,
                    color: '#38bdf8',
                    items,
                    createdBy: APP.state.displayName,
                    createdAt: Date.now()
                });
            }
            saveToStorage(APP.roomKey('vcc_checklist_templates'), APP.state.checklistTemplates);
            self.renderTemplates();
            overlay.remove();
            APP.showStatusMsg(isEdit ? '✅ 範本已更新' : '✅ 範本已建立');
        });
        overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
        document.getElementById('tplFormTitle').focus();
    }
}
